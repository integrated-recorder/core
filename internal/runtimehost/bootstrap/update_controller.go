package bootstrap

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/buildinfo"
	"github.com/integrated-recorder/core/internal/controlplane"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/runtimehook"
	"github.com/integrated-recorder/core/internal/runtimehost/adaptercatalog"
	"github.com/integrated-recorder/core/internal/runtimehost/generation"
	"github.com/integrated-recorder/core/internal/runtimehost/httpapi"
	"github.com/integrated-recorder/core/internal/runtimehost/install"
	"github.com/integrated-recorder/core/internal/runtimehost/installation"
	"github.com/integrated-recorder/core/internal/runtimehost/leases"
	"github.com/integrated-recorder/core/internal/runtimehost/pluginregistry"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/runtimehost/release"
	"github.com/integrated-recorder/core/internal/runtimehost/resources"
	"github.com/integrated-recorder/core/internal/runtimehost/storagecatalog"
	"github.com/integrated-recorder/core/internal/runtimehost/supervisor"
	"github.com/integrated-recorder/core/internal/runtimeipc"
)

const (
	updateOperationTimeout = 2 * time.Minute
	updateMaxTrustKeys     = 16
)

type updateSupervisor interface {
	Snapshot() supervisor.Snapshot
	StartEngine(context.Context, supervisor.ProcessSpec) error
	StartControl(context.Context, supervisor.ProcessSpec, *url.URL) error
	ActivateControlWith(context.Context, string, func() error, func() error) error
	StopControl(context.Context, string) error
	RetireEngine(context.Context, string, supervisor.EngineDrain) error
}

type readinessRegistrar interface {
	supervisor.Readiness
	register(string, supervisor.Role, string, string)
	engineIdentity(string) (recorderengine.ReadyResult, error)
	controlIdentity(string) (controlplane.LifecycleSnapshot, error)
}

type lifecycleRegistrar interface {
	supervisor.ControlLifecycle
	register(string, string, string, string) error
}

type engineDrainRegistrar interface {
	supervisor.EngineDrain
	register(string, string, string, string) error
}

type hostAdapterCatalog interface {
	Reconcile(context.Context, string) (adaptercatalog.Snapshot, error)
	Empty() (adaptercatalog.Snapshot, error)
	Load(string) (adaptercatalog.Snapshot, error)
	Collect([]string) error
}

// EngineDetacher removes a confirmed-idle Engine from the running active
// Control's runtime catalog before the Host retires that Engine process.
type EngineDetacher interface {
	DetachEngine(context.Context, string, string) error
}

// engineAttachment is private Host runtime state. Paths and tokens in this
// value are never projected through the update API.
type engineAttachment struct {
	generationID         string
	resourceOwnerID      string
	releaseDir           string
	adapterSetID         string
	storageProviderSetID string
	socketPath           string
	tokenPath            string
	instanceID           string
	manifest             *release.Manifest
}

type updateControllerOptions struct {
	Config            Config
	HostBuild         buildinfo.Info
	ApplicationBuild  buildinfo.Info
	Registry          *generation.Registry
	AdapterCatalog    hostAdapterCatalog
	PluginRegistry    *pluginregistry.Manager
	StorageCatalog    *storagecatalog.Catalog
	Installation      *installation.Store
	Supervisor        updateSupervisor
	Readiness         readinessRegistrar
	Lifecycle         lifecycleRegistrar
	Drain             engineDrainRegistrar
	EngineDetacher    EngineDetacher
	Coordinator       *resources.Coordinator
	OwnerAuthority    *resources.RecordingOwnerAuthority
	Installer         *install.Installer
	TrustedKeys       map[string]ed25519.PublicKey
	Compatibility     release.HostCompatibility
	SourceFactory     func() (install.Source, error)
	ControlTarget     func() (string, *url.URL, error)
	CatalogWriter     func(string, controlplane.EngineCatalog) error
	Engines           []engineAttachment
	ActiveControlID   string
	ResourceSocket    string
	ResourceTokenPath string
	UnavailableReason string
	OperationTimeout  time.Duration
}

type updateController struct {
	config            Config
	hostBuild         buildinfo.Info
	applicationBuild  buildinfo.Info
	registry          *generation.Registry
	adapterCatalog    hostAdapterCatalog
	pluginRegistry    *pluginregistry.Manager
	storageCatalog    *storagecatalog.Catalog
	installation      *installation.Store
	supervisor        updateSupervisor
	readiness         readinessRegistrar
	lifecycle         lifecycleRegistrar
	drain             engineDrainRegistrar
	engineDetacher    EngineDetacher
	coordinator       *resources.Coordinator
	ownerAuthority    *resources.RecordingOwnerAuthority
	installer         *install.Installer
	trustedKeys       map[string]ed25519.PublicKey
	compatibility     release.HostCompatibility
	sourceFactory     func() (install.Source, error)
	controlTarget     func() (string, *url.URL, error)
	catalogWriter     func(string, controlplane.EngineCatalog) error
	resourceSocket    string
	resourceTokenPath string
	operationTimeout  time.Duration
	unavailableReason string

	gate chan struct{}
	mu   sync.RWMutex

	engines         map[string]engineAttachment
	manifests       map[string]release.Manifest
	releaseNotes    map[string]string
	buildIdentities map[string]buildinfo.Info
	bundleBuild     buildinfo.Info
	inventorySource leases.InventorySource
	available       *release.Manifest
	availableSource install.Source
	availableNotes  string
	verification    string
	lastFailureCode string
}

var _ httpapi.Controller = (*updateController)(nil)

func newUpdateController(options updateControllerOptions) (*updateController, error) {
	if err := options.Config.Validate(); err != nil || options.Registry == nil || options.Supervisor == nil || options.Readiness == nil || options.Lifecycle == nil || options.Drain == nil || options.Coordinator == nil {
		return nil, errors.New("runtime update controller dependencies are incomplete")
	}
	if options.ActiveControlID != "" {
		if !generationPattern.MatchString(options.ActiveControlID) || options.Supervisor.Snapshot().ActiveControlGeneration != options.ActiveControlID {
			return nil, errors.New("runtime update active Control identity is inconsistent")
		}
		if activeID := options.Registry.Snapshot().ActiveGenerationID; activeID != "" && activeID != options.ActiveControlID {
			return nil, errors.New("runtime update active generation identity is inconsistent")
		}
	}
	if options.UnavailableReason == "" {
		switch {
		case options.HostBuild.Version == "dev" || options.ApplicationBuild.Version == "dev":
			options.UnavailableReason = "development_build"
		case len(options.TrustedKeys) == 0 || options.Installer == nil:
			options.UnavailableReason = "trust_key_unavailable"
		case options.SourceFactory == nil:
			options.UnavailableReason = "source_unavailable"
		}
	}
	if len(options.TrustedKeys) > updateMaxTrustKeys {
		return nil, errors.New("runtime update trust key count exceeds limit")
	}
	if options.OperationTimeout == 0 {
		options.OperationTimeout = updateOperationTimeout
	}
	if options.OperationTimeout < time.Second || options.OperationTimeout > 5*time.Minute {
		return nil, errors.New("runtime update operation timeout is invalid")
	}
	if options.ControlTarget == nil {
		options.ControlTarget = privateControlTarget
	}
	if options.CatalogWriter == nil {
		options.CatalogWriter = writeEngineCatalog
	}
	if options.ResourceSocket != "" && validateSocketPath(options.ResourceSocket) != nil {
		return nil, errors.New("runtime update resource socket is invalid")
	}
	if options.ResourceTokenPath != "" && (!filepath.IsAbs(options.ResourceTokenPath) || filepath.Clean(options.ResourceTokenPath) != options.ResourceTokenPath) {
		return nil, errors.New("runtime update resource credential path is invalid")
	}
	keys := make(map[string]ed25519.PublicKey, len(options.TrustedKeys))
	for keyID, publicKey := range options.TrustedKeys {
		if len(keyID) == 0 || len(keyID) > 64 || len(publicKey) != ed25519.PublicKeySize {
			return nil, errors.New("runtime update trust key is invalid")
		}
		keys[keyID] = append(ed25519.PublicKey(nil), publicKey...)
	}
	c := &updateController{
		config: options.Config, hostBuild: options.HostBuild, applicationBuild: options.ApplicationBuild,
		registry: options.Registry, adapterCatalog: options.AdapterCatalog, pluginRegistry: options.PluginRegistry, storageCatalog: options.StorageCatalog, installation: options.Installation,
		supervisor: options.Supervisor, readiness: options.Readiness,
		lifecycle: options.Lifecycle, drain: options.Drain, engineDetacher: options.EngineDetacher, coordinator: options.Coordinator,
		ownerAuthority: options.OwnerAuthority,
		installer:      options.Installer, trustedKeys: keys, compatibility: options.Compatibility,
		sourceFactory: options.SourceFactory, resourceSocket: options.ResourceSocket,
		catalogWriter:     options.CatalogWriter,
		resourceTokenPath: options.ResourceTokenPath, operationTimeout: options.OperationTimeout,
		controlTarget:     options.ControlTarget,
		unavailableReason: options.UnavailableReason, gate: make(chan struct{}, 1),
		engines: make(map[string]engineAttachment), manifests: make(map[string]release.Manifest), buildIdentities: make(map[string]buildinfo.Info),
		bundleBuild:  options.ApplicationBuild,
		verification: "not_checked",
	}
	for _, attachment := range options.Engines {
		if !generationPattern.MatchString(attachment.generationID) || !filepath.IsAbs(attachment.releaseDir) || !filepath.IsAbs(attachment.socketPath) || !filepath.IsAbs(attachment.tokenPath) || attachment.instanceID == "" {
			return nil, errors.New("initial engine attachment is invalid")
		}
		if _, exists := c.engines[attachment.generationID]; exists {
			return nil, errors.New("initial engine attachment is duplicated")
		}
		c.engines[attachment.generationID] = attachment
		if attachment.manifest != nil {
			c.manifests[attachment.generationID] = *attachment.manifest
			c.buildIdentities[attachment.generationID] = buildInfoFromManifest(*attachment.manifest)
		}
	}
	if activeID := options.Registry.Snapshot().ActiveGenerationID; activeID != "" && options.ApplicationBuild.Version != "" {
		if _, known := c.buildIdentities[activeID]; !known {
			c.buildIdentities[activeID] = options.ApplicationBuild
		}
	}
	if c.unavailableReason == "" && (c.installer == nil || c.sourceFactory == nil || len(c.trustedKeys) == 0) {
		if len(c.trustedKeys) == 0 || c.installer == nil {
			c.unavailableReason = "trust_key_unavailable"
		} else {
			c.unavailableReason = "source_unavailable"
		}
	}
	if c.unavailableReason == "" && (c.resourceSocket == "" || c.resourceTokenPath == "") {
		return nil, errors.New("runtime update requires the Host resource coordinator IPC")
	}
	return c, nil
}

func (c *updateController) Status(ctx context.Context) (httpapi.Status, error) {
	if ctx == nil {
		return httpapi.Status{}, httpapi.NewControllerError("internal_error")
	}
	if err := ctx.Err(); err != nil {
		return httpapi.Status{}, httpapi.NewControllerError("internal_error")
	}
	return c.status(), nil
}

func (c *updateController) Check(ctx context.Context) (httpapi.Status, error) {
	if err := c.acquireOperation(ctx); err != nil {
		return httpapi.Status{}, err
	}
	defer c.releaseOperation()
	if err := c.availableForUpdates(); err != nil {
		return httpapi.Status{}, err
	}
	manifest, src, err := c.fetchCandidate(ctx)
	if err != nil {
		return httpapi.Status{}, err
	}
	if c.activeMatches(manifest) {
		c.mu.Lock()
		c.available, c.availableSource, c.availableNotes = nil, nil, ""
		c.verification, c.lastFailureCode = "verified", "no_update_available"
		c.mu.Unlock()
		return c.status(), httpapi.NewControllerError("no_update_available")
	}
	c.mu.Lock()
	copyManifest := manifest
	c.available, c.availableSource = &copyManifest, src
	c.availableNotes = releaseNotesSummary(src)
	c.verification, c.lastFailureCode = "verified", ""
	c.mu.Unlock()
	return c.status(), nil
}

func (c *updateController) Stage(ctx context.Context) (httpapi.Status, error) {
	if err := c.acquireOperation(ctx); err != nil {
		return httpapi.Status{}, err
	}
	defer c.releaseOperation()
	if err := c.availableForUpdates(); err != nil {
		return httpapi.Status{}, err
	}
	snapshot := c.registry.Snapshot()
	if snapshot.StagedGenerationID != "" {
		return httpapi.Status{}, httpapi.NewControllerError("operation_conflict")
	}
	manifest, src := c.cachedAvailable()
	if manifest == nil || src == nil {
		fetched, fetchedSource, err := c.fetchCandidate(ctx)
		if err != nil {
			return httpapi.Status{}, err
		}
		if c.activeMatches(fetched) {
			return httpapi.Status{}, httpapi.NewControllerError("no_update_available")
		}
		manifest, src = &fetched, fetchedSource
	}
	stageCtx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()
	staged, err := c.installer.Stage(stageCtx, src)
	if errors.Is(err, install.ErrReleaseExists) {
		id := install.ReleaseDirectoryID(manifest.ReleaseVersion, manifest.Commit)
		installed, inspectErr := install.InspectInstalledRelease(filepath.Join(c.config.DataDir, "runtime"), id, c.trustedKeys, c.compatibility)
		if inspectErr != nil {
			return httpapi.Status{}, c.fail("verification_failed")
		}
		staged = install.StagedRelease{ID: installed.ID, Version: installed.Manifest.ReleaseVersion, Commit: installed.Manifest.Commit, Channel: installed.Manifest.Channel, Verified: true, ArtifactCount: len(installed.Manifest.Artifacts)}
		err = nil
	}
	if err != nil {
		return httpapi.Status{}, c.mapInstallError(err, "stage_failed")
	}
	if !staged.Verified || staged.ID != install.ReleaseDirectoryID(manifest.ReleaseVersion, manifest.Commit) {
		return httpapi.Status{}, c.fail("verification_failed")
	}
	installed, err := install.InspectInstalledRelease(filepath.Join(c.config.DataDir, "runtime"), staged.ID, c.trustedKeys, c.compatibility)
	if err != nil {
		return httpapi.Status{}, c.mapInstallError(err, "verification_failed")
	}
	if manifest == nil || !reflect.DeepEqual(*manifest, installed.Manifest) {
		return httpapi.Status{}, c.fail("verification_failed")
	}
	activeGeneration, hasActive := snapshot.Generations[snapshot.ActiveGenerationID]
	if !hasActive || activeGeneration.State != generation.StateActive {
		return httpapi.Status{}, httpapi.NewControllerError("stage_failed")
	}
	genID, err := newGenerationID()
	if err != nil {
		return httpapi.Status{}, c.fail("internal_error")
	}
	newGeneration := generation.Generation{
		ID: genID, Version: installed.Manifest.ReleaseVersion, Commit: strings.ToLower(installed.Manifest.Commit),
		AdapterSetID: activeGeneration.AdapterSetID, StorageProviderSetID: activeGeneration.StorageProviderSetID,
		InstalledAt: time.Now().UTC(), State: generation.StateStaging,
		ControlProtocol: installed.Manifest.ControlProtocolVersion, EngineProtocol: installed.Manifest.EngineProtocolVersion,
		ArchiveReadCompatibility: generation.CompatibilityRange{Minimum: installed.Manifest.ArchiveReadMinimum, Maximum: installed.Manifest.ArchiveReadMaximum},
		ArchiveWriteEpoch:        installed.Manifest.ArchiveWriteEpoch,
	}
	if err := c.registry.Stage(newGeneration); err != nil {
		return httpapi.Status{}, c.mapRegistryError(err, "stage_failed")
	}
	if err := c.registry.MarkVerified(genID); err != nil {
		_ = c.registry.Fail(genID)
		return httpapi.Status{}, c.mapRegistryError(err, "stage_failed")
	}
	c.mu.Lock()
	c.manifests[genID] = installed.Manifest
	if c.releaseNotes == nil {
		c.releaseNotes = make(map[string]string)
	}
	if notes := releaseNotesSummary(src); notes != "" {
		c.releaseNotes[genID] = notes
	}
	c.available, c.availableSource, c.availableNotes = nil, nil, ""
	c.verification, c.lastFailureCode = "verified", ""
	c.mu.Unlock()
	return c.status(), nil
}

func (c *updateController) Activate(ctx context.Context) (httpapi.Status, error) {
	if err := c.acquireOperation(ctx); err != nil {
		return httpapi.Status{}, err
	}
	defer c.releaseOperation()
	if err := c.availableForUpdates(); err != nil {
		return httpapi.Status{}, err
	}
	snapshot := c.registry.Snapshot()
	id := snapshot.StagedGenerationID
	candidate, exists := snapshot.Generations[id]
	if id == "" || !exists || candidate.State != generation.StateVerified {
		return httpapi.Status{}, httpapi.NewControllerError("candidate_not_ready")
	}
	installed, err := c.inspectGeneration(candidate)
	if err != nil {
		return httpapi.Status{}, c.mapInstallError(err, "verification_failed")
	}
	if !c.archiveCompatibleWithRuntime(installed.Manifest) {
		return httpapi.Status{}, c.fail("release_incompatible")
	}
	ctx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()
	attachment, err := c.startEngine(ctx, candidate.ID, installed.Directory, installed.Manifest, installed.ArtifactPaths)
	if err != nil {
		c.failCandidate(ctx, candidate.ID)
		return httpapi.Status{}, c.mapLaunchError(err, "candidate_not_ready")
	}
	if err := c.registerEngine(candidate.ID, attachment); err != nil {
		c.failCandidate(ctx, candidate.ID)
		return httpapi.Status{}, c.fail("candidate_not_ready")
	}
	if err := c.publishControl(ctx, candidate.ID, installed.Directory, installed.Manifest, installed.ArtifactPaths, candidate.ID); err != nil {
		c.failCandidate(ctx, candidate.ID)
		return httpapi.Status{}, c.mapLaunchError(err, "candidate_not_ready")
	}
	if err := c.registry.MarkReady(candidate.ID); err != nil {
		c.failCandidate(ctx, candidate.ID)
		return httpapi.Status{}, c.mapRegistryError(err, "activation_failed")
	}
	previousID := snapshot.ActiveGenerationID
	activateErr := c.supervisor.ActivateControlWith(ctx, candidate.ID,
		func() error { return c.registry.Activate(candidate.ID) },
		func() error { return c.registry.AbortActivation(candidate.ID, previousID) },
	)
	current := c.registry.Snapshot()
	if current.ActiveGenerationID == candidate.ID && c.supervisor.Snapshot().ActiveControlGeneration == candidate.ID {
		if err := c.registry.FinalizeActivation(candidate.ID); err != nil {
			c.mu.Lock()
			c.lastFailureCode = "internal_error"
			c.mu.Unlock()
		}
		c.mu.Lock()
		c.applicationBuild = buildInfoFromManifest(installed.Manifest)
		c.manifests[candidate.ID] = installed.Manifest
		c.buildIdentities[candidate.ID] = c.applicationBuild
		c.verification, c.lastFailureCode = "verified", ""
		c.mu.Unlock()
		// A bounded old-Control drain may remain pending after the route and new
		// epoch are active. The candidate is still the committed default.
		return c.status(), nil
	}
	if activateErr != nil {
		c.failCandidate(ctx, candidate.ID)
		return httpapi.Status{}, httpapi.NewControllerError("activation_failed")
	}
	c.failCandidate(ctx, candidate.ID)
	return httpapi.Status{}, httpapi.NewControllerError("activation_failed")
}

func (c *updateController) Rollback(ctx context.Context) (httpapi.Status, error) {
	if err := c.acquireOperation(ctx); err != nil {
		return httpapi.Status{}, err
	}
	defer c.releaseOperation()
	if err := c.availableForUpdates(); err != nil {
		return httpapi.Status{}, err
	}
	snapshot := c.registry.Snapshot()
	previousID := snapshot.PreviousGenerationID
	previous, exists := snapshot.Generations[previousID]
	if previousID == "" || !exists || previous.State != generation.StateDraining {
		return httpapi.Status{}, httpapi.NewControllerError("rollback_unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()
	installed, _, err := c.loadGeneration(previous)
	if err != nil {
		return httpapi.Status{}, c.mapInstallError(err, "rollback_unavailable")
	}
	if installed != nil && !c.archiveCompatibleWithRuntime(installed.Manifest) {
		return httpapi.Status{}, c.fail("release_incompatible")
	}
	attachment, attached := c.engineAttachment(previousID)
	engineState := c.supervisorEngineState(previousID)
	if !attached || engineState != supervisor.ProcessReady {
		if engineState == supervisor.ProcessStarting || engineState == supervisor.ProcessStopping {
			return httpapi.Status{}, c.fail("rollback_failed")
		}
		if !attached || engineState == supervisor.ProcessExited || engineState == supervisor.ProcessFailed {
			var paths map[string]string
			var directory string
			if installed != nil {
				directory, paths = installed.Directory, installed.ArtifactPaths
			} else {
				directory, paths = c.bundleExecutables()
			}
			manifest := release.Manifest{}
			if installed != nil {
				manifest = installed.Manifest
			}
			attachment, err = c.startEngine(ctx, previousID, directory, manifest, paths)
			if err != nil {
				return httpapi.Status{}, c.mapLaunchError(err, "rollback_failed")
			}
			if err := c.registerEngine(previousID, attachment); err != nil {
				return httpapi.Status{}, c.fail("rollback_failed")
			}
		} else {
			return httpapi.Status{}, c.fail("rollback_failed")
		}
	}
	var directory string
	var paths map[string]string
	var manifest release.Manifest
	if installed != nil {
		directory, paths, manifest = installed.Directory, installed.ArtifactPaths, installed.Manifest
	} else {
		directory, paths = c.bundleExecutables()
	}
	if err := c.publishControl(ctx, previousID, directory, manifest, paths, previousID); err != nil {
		_ = c.supervisor.StopControl(context.Background(), previousID)
		return httpapi.Status{}, c.mapLaunchError(err, "rollback_failed")
	}
	rollbackErr := c.supervisor.ActivateControlWith(ctx, previousID,
		func() error { return c.registry.Rollback() },
		func() error { return c.registry.Rollback() },
	)
	current := c.registry.Snapshot()
	if current.ActiveGenerationID == previousID && c.supervisor.Snapshot().ActiveControlGeneration == previousID {
		if installed != nil {
			c.mu.Lock()
			c.applicationBuild = buildInfoFromManifest(manifest)
			c.manifests[previousID] = manifest
			c.buildIdentities[previousID] = c.applicationBuild
			c.mu.Unlock()
		} else {
			c.mu.Lock()
			if build, ok := c.buildIdentities[previousID]; ok {
				c.applicationBuild = build
			} else if matchesBuild(previous, c.hostBuild) {
				c.applicationBuild = c.hostBuild
			}
			c.mu.Unlock()
		}
		c.mu.Lock()
		c.verification, c.lastFailureCode = "verified", ""
		c.mu.Unlock()
		_ = rollbackErr
		return c.status(), nil
	}
	_ = rollbackErr
	_ = c.supervisor.StopControl(context.Background(), previousID)
	return httpapi.Status{}, httpapi.NewControllerError("rollback_failed")
}

// ReconcileAdapters imports the configured trusted-local sources and, when
// their immutable set changes, activates a new application generation bound to
// that set. It shares the update gate so a release tuple cannot be mixed.
func (c *updateController) ReconcileAdapters(ctx context.Context) (httpapi.AdapterReconcileResult, error) {
	if ctx == nil {
		return httpapi.AdapterReconcileResult{}, httpapi.NewControllerError("internal_error")
	}
	operationCtx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()
	result, _, err := c.reconcileAdapters(operationCtx, true)
	return result, err
}

// reconcileAdaptersIfIdle is used only by the periodic Host loop. It skips a
// tick when an update, lease reconciliation, or explicit adapter operation
// already owns the common controller gate.
func (c *updateController) reconcileAdaptersIfIdle(ctx context.Context) (bool, error) {
	_, ran, err := c.reconcileAdapters(ctx, false)
	return ran, err
}

func (c *updateController) reconcileAdapters(ctx context.Context, wait bool) (httpapi.AdapterReconcileResult, bool, error) {
	return c.reconcileAdaptersWithGate(ctx, wait, false)
}

// reconcileAdaptersLocked runs adapter-set reconciliation while the caller
// already owns the common app-update/plugin-operation gate.
func (c *updateController) reconcileAdaptersLocked(ctx context.Context) (httpapi.AdapterReconcileResult, error) {
	result, _, err := c.reconcileAdaptersWithGate(ctx, true, true)
	return result, err
}

func (c *updateController) reconcileAdaptersWithGate(ctx context.Context, wait, gateHeld bool) (httpapi.AdapterReconcileResult, bool, error) {
	if ctx == nil {
		return httpapi.AdapterReconcileResult{}, false, httpapi.NewControllerError("internal_error")
	}
	if c.installation == nil || c.installation.Snapshot().State != installation.StateReady {
		return httpapi.AdapterReconcileResult{}, false, httpapi.NewControllerError("installation_incomplete")
	}
	if c.adapterCatalog == nil {
		return httpapi.AdapterReconcileResult{State: "failed", FailureCode: "reconcile_failed"}, false, nil
	}
	if !gateHeld {
		if err := c.lockOperation(ctx, wait); err != nil {
			if !wait {
				return httpapi.AdapterReconcileResult{}, false, nil
			}
			return httpapi.AdapterReconcileResult{}, false, httpapi.NewControllerError("operation_conflict")
		}
		defer c.releaseOperation()
	}
	if err := ctx.Err(); err != nil {
		return httpapi.AdapterReconcileResult{State: "failed", FailureCode: "reconcile_failed"}, true, nil
	}

	registryState := c.registry.Snapshot()
	active, exists := registryState.Generations[registryState.ActiveGenerationID]
	if !exists || active.State != generation.StateActive {
		return httpapi.AdapterReconcileResult{State: "failed", FailureCode: "reconcile_failed"}, true, nil
	}
	// A staged application release already owns the single candidate slot. Do
	// not reconcile the catalog first: importing a changed adapter set can
	// publish immutable artifacts even though it cannot be attached to another
	// staged generation. Explicit requests report a conflict; the periodic loop
	// simply waits for the staged operation to finish.
	if registryState.StagedGenerationID != "" {
		if wait {
			return httpapi.AdapterReconcileResult{}, false, httpapi.NewControllerError("operation_conflict")
		}
		return httpapi.AdapterReconcileResult{}, false, nil
	}
	fallbackSetID := active.AdapterSetID
	if fallbackSetID == "" {
		empty, err := c.adapterCatalog.Empty()
		if err != nil {
			return httpapi.AdapterReconcileResult{State: "failed", FailureCode: "reconcile_failed"}, true, nil
		}
		fallbackSetID = empty.ID
	}
	selected, err := c.reconcileCatalog(ctx, fallbackSetID)
	if err != nil {
		return httpapi.AdapterReconcileResult{State: "failed", FailureCode: "reconcile_failed"}, true, nil
	}
	activeCount := 0
	if active.AdapterSetID != "" {
		if current, loadErr := c.adapterCatalog.Load(active.AdapterSetID); loadErr == nil {
			activeCount = len(current.Entries)
		}
	}
	if selected.ID == active.AdapterSetID {
		state := "unchanged"
		if selected.RejectedCount > 0 {
			state = "rejected"
		}
		return httpapi.AdapterReconcileResult{State: state, ActiveAdapterCount: activeCount, RejectedCount: selected.RejectedCount}, true, nil
	}
	genID, err := newGenerationID()
	if err != nil {
		return httpapi.AdapterReconcileResult{State: "failed", ActiveAdapterCount: activeCount, FailureCode: "reconcile_failed"}, true, nil
	}
	candidate := active
	candidate.ID = genID
	candidate.AdapterSetID = selected.ID
	candidate.InstalledAt = time.Now().UTC()
	candidate.State = generation.StateStaging
	candidate.EngineDormant = false
	if err := c.registry.Stage(candidate); err != nil {
		return httpapi.AdapterReconcileResult{State: "failed", ActiveAdapterCount: activeCount, FailureCode: "reconcile_failed"}, true, nil
	}
	if err := c.registry.MarkVerified(genID); err != nil {
		_ = c.registry.Fail(genID)
		return httpapi.AdapterReconcileResult{State: "failed", ActiveAdapterCount: activeCount, FailureCode: "reconcile_failed"}, true, nil
	}
	directory, manifest, paths, err := c.applicationFiles(active)
	if err != nil {
		c.failCandidate(ctx, genID)
		return httpapi.AdapterReconcileResult{State: "failed", ActiveAdapterCount: activeCount, FailureCode: "candidate_not_ready"}, true, nil
	}
	operationCtx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()
	attachment, err := c.startEngine(operationCtx, genID, directory, manifest, paths)
	if err == nil {
		err = c.registerEngine(genID, attachment)
	}
	if err == nil {
		err = c.publishControl(operationCtx, genID, directory, manifest, paths, genID)
	}
	if err == nil {
		err = c.registry.MarkReady(genID)
	}
	if err != nil {
		c.failCandidate(operationCtx, genID)
		return httpapi.AdapterReconcileResult{State: "failed", ActiveAdapterCount: activeCount, FailureCode: "candidate_not_ready"}, true, nil
	}
	activationErr := c.supervisor.ActivateControlWith(operationCtx, genID,
		func() error { return c.registry.Activate(genID) },
		func() error { return c.registry.AbortActivation(genID, active.ID) },
	)
	current := c.registry.Snapshot()
	if current.ActiveGenerationID != genID || c.supervisor.Snapshot().ActiveControlGeneration != genID {
		c.failCandidate(operationCtx, genID)
		_ = activationErr
		return httpapi.AdapterReconcileResult{State: "failed", ActiveAdapterCount: activeCount, FailureCode: "activation_failed"}, true, nil
	}
	if err := c.registry.FinalizeActivation(genID); err != nil {
		// The durable activation journal is recovered by Host startup. The new
		// route is already active, so report committed success without tearing it
		// down or disturbing its Engine.
	}
	c.mu.Lock()
	if manifest.ReleaseVersion != "" {
		c.manifests[genID] = manifest
		c.buildIdentities[genID] = buildInfoFromManifest(manifest)
	}
	c.mu.Unlock()
	return httpapi.AdapterReconcileResult{State: "activated", ActiveAdapterCount: len(selected.Entries), RejectedCount: selected.RejectedCount, GenerationID: genID}, true, nil
}

func (c *updateController) applicationFiles(active generation.Generation) (string, release.Manifest, map[string]string, error) {
	if attachment, ok := c.engineAttachment(active.ID); ok && attachment.releaseDir == c.config.BundleDir {
		directory, paths := c.bundleExecutables()
		manifest := release.Manifest{}
		if attachment.manifest != nil {
			manifest = *attachment.manifest
		}
		return directory, manifest, paths, nil
	}
	installed, err := c.inspectGeneration(active)
	if err != nil {
		return "", release.Manifest{}, nil, err
	}
	return installed.Directory, installed.Manifest, installed.ArtifactPaths, nil
}

// ReconcileLeases performs one complete confirmed inventory pass over every
// running active or draining Engine. A failed Engine inventory leaves the
// previous durable lease projection unchanged.
func (c *updateController) ReconcileLeases(ctx context.Context) error {
	if ctx == nil {
		return errors.New("lease reconciliation context is required")
	}
	if err := c.lockOperation(ctx, false); err != nil {
		return err
	}
	defer c.releaseOperation()
	attachments := c.EngineAttachments()
	sup := c.supervisor.Snapshot()
	ready := make(map[string]bool, len(sup.Generations))
	processStates := make(map[string]supervisor.ProcessState, len(sup.Generations))
	processIDs := make([]string, 0, len(sup.Generations))
	for _, process := range sup.Generations {
		ready[process.ID] = process.Engine.State == supervisor.ProcessReady
		processStates[process.ID] = process.Engine.State
		processIDs = append(processIDs, process.ID)
	}
	eligible := make(map[string]engineAttachment)
	for _, attachment := range attachments {
		if ready[attachment.generationID] {
			eligible[attachment.generationID] = attachment
		}
	}
	source := c.inventorySource
	if source == nil {
		source = leases.InventorySourceFunc(func(ctx context.Context, engine generation.Generation) (generation.EngineInventory, error) {
			attachment, ok := eligible[engine.ID]
			if !ok {
				attachmentIDs := make([]string, 0, len(attachments))
				for _, item := range attachments {
					attachmentIDs = append(attachmentIDs, item.generationID)
				}
				return generation.EngineInventory{}, fmt.Errorf("engine inventory endpoint is unavailable (generation=%s process_state=%s process_ids=%v attachment_ids=%v)", engine.ID, processStates[engine.ID], processIDs, attachmentIDs)
			}
			secret, err := controlplane.LoadPrivateIPCSecret(attachment.tokenPath)
			if err != nil {
				return generation.EngineInventory{}, err
			}
			client, err := runtimeipc.NewClientForInstance(attachment.socketPath, attachment.generationID, attachment.instanceID, secret, 3*time.Second)
			if err != nil {
				return generation.EngineInventory{}, err
			}
			manager, err := recorderengine.NewManagerClient(client)
			if err != nil {
				return generation.EngineInventory{}, err
			}
			inventory, err := manager.Inventory(ctx)
			if err != nil || !inventory.Ready || inventory.GenerationID != engine.ID || inventory.InstanceID != attachment.instanceID {
				return generation.EngineInventory{}, errors.New("engine inventory identity could not be confirmed")
			}
			result := generation.EngineInventory{Confirmed: true, EngineGeneration: engine.ID, WorkerInstance: inventory.InstanceID, ObservedAt: time.Now().UTC(), Recordings: make([]generation.InventoryRecording, 0, len(inventory.Active))}
			for _, active := range inventory.Active {
				if c.ownerAuthority != nil {
					if active.Owner == nil {
						return generation.EngineInventory{}, errors.New("engine inventory omitted recording owner")
					}
					if !c.ownerAuthority.MatchesDurableOwner(*active.Owner) {
						// A parked source can remain visible briefly after the Host has
						// transferred its durable owner. Do not let stale inventory
						// move the retirement lease back to that source.
						leaseState := c.registry.Snapshot().Leases[active.RecordingID]
						if leaseState.EngineGeneration != "" && leaseState.EngineGeneration != engine.ID && active.Owner.EngineGeneration == engine.ID {
							// A lost response to the source-detach IPC is retried here.
							// CompleteHandover is accepted only for a parked stale worker;
							// the Engine independently checks the durable owner fence.
							_ = manager.CompleteHandover(ctx, active.RecordingID, *active.Owner)
						}
						continue
					}
				}
				result.Recordings = append(result.Recordings, generation.InventoryRecording{RecordingID: active.RecordingID, StartedAt: active.StartedAt})
			}
			return result, nil
		})
	}
	reconcile := func(protected []generation.Lease) error {
		protectedByGeneration := make(map[string][]generation.Lease)
		for _, lease := range protected {
			protectedByGeneration[lease.EngineGeneration] = append(protectedByGeneration[lease.EngineGeneration], lease)
		}
		protectedSource := leases.InventorySourceFunc(func(ctx context.Context, engine generation.Generation) (generation.EngineInventory, error) {
			inventory, err := source.Inventory(ctx, engine)
			if err != nil {
				return generation.EngineInventory{}, err
			}
			seen := make(map[string]struct{}, len(inventory.Recordings))
			for _, item := range inventory.Recordings {
				seen[item.RecordingID] = struct{}{}
			}
			for _, lease := range protectedByGeneration[engine.ID] {
				if lease.WorkerInstance != inventory.WorkerInstance {
					continue
				}
				if _, present := seen[lease.RecordingID]; present {
					continue
				}
				inventory.Recordings = append(inventory.Recordings, generation.InventoryRecording{RecordingID: lease.RecordingID, StartedAt: lease.StartedAt})
			}
			return inventory, nil
		})
		reconciler, err := leases.New(c.registry, protectedSource)
		if err != nil {
			return err
		}
		return reconciler.RunOnce(ctx)
	}
	if c.ownerAuthority != nil {
		if err := c.ownerAuthority.WithProtectedLeases(reconcile); err != nil {
			return err
		}
	} else if err := reconcile(nil); err != nil {
		return err
	}
	// Application activation and per-Recording handover are separate failure
	// domains. Try at most one eligible Recording per inventory pass; a failed
	// target leaves the source Engine as owner and does not roll back the active
	// application generation.
	if err := c.handoverOneDrainingRecording(ctx, eligible); err != nil {
		slog.Warn("active recording handover failed; source ownership retained when provable",
			"stage", "orchestration", "code", "handover_failed")
	}
	return c.retireSafeGenerations(ctx)
}

// handoverOneDrainingRecording transfers at most one Recording from a draining
// Engine to the active Engine during a reconciliation pass. Adapter Protocol
// v1 has no compatibility declaration for changing an adapter binary while a
// Recording is live, so the immutable adapter-set identity must be identical.
func (c *updateController) handoverOneDrainingRecording(ctx context.Context, ready map[string]engineAttachment) error {
	if c.ownerAuthority == nil || ctx == nil {
		return nil
	}
	registryState := c.registry.Snapshot()
	targetID := registryState.ActiveGenerationID
	target, ok := ready[targetID]
	if !ok || targetID == "" || target.adapterSetID == "" {
		return nil
	}
	targetGeneration, ok := registryState.Generations[targetID]
	if !ok || targetGeneration.State != generation.StateActive {
		return nil
	}

	sourceIDs := make([]string, 0, len(registryState.Generations))
	for id, sourceGeneration := range registryState.Generations {
		if id == targetID || sourceGeneration.State != generation.StateDraining || sourceGeneration.AdapterSetID == "" || sourceGeneration.AdapterSetID != targetGeneration.AdapterSetID || sourceGeneration.StorageProviderSetID != targetGeneration.StorageProviderSetID {
			continue
		}
		if _, live := ready[id]; live {
			sourceIDs = append(sourceIDs, id)
		}
	}
	sort.Strings(sourceIDs)
	for _, sourceID := range sourceIDs {
		leases := make([]generation.Lease, 0)
		for _, lease := range registryState.Leases {
			if lease.EngineGeneration == sourceID {
				leases = append(leases, lease)
			}
		}
		sort.Slice(leases, func(i, j int) bool { return leases[i].RecordingID < leases[j].RecordingID })
		for _, lease := range leases {
			if err := ctx.Err(); err != nil {
				return err
			}
			owner, err := c.ownerAuthority.CurrentOwner(lease.RecordingID)
			if err != nil || owner.EngineGeneration != sourceID || owner.WorkerInstance != lease.WorkerInstance {
				continue
			}
			source := ready[sourceID]
			if err := c.handoverRecording(ctx, source, target, lease, owner); err != nil {
				return err
			}
			return nil
		}
	}
	return nil
}

func (c *updateController) handoverRecording(ctx context.Context, source, target engineAttachment, lease generation.Lease, owner recordingowner.Owner) error {
	started := time.Now()
	stage := "inventory"
	defer func() {
		// Error details can contain adapter-controlled strings, so this event
		// records only bounded identities and a stable safe code.
		if stage != "complete" && stage != "aborted" {
			slog.Warn("recording handover_failed", "recording_id", lease.RecordingID, "source_generation", source.generationID, "target_generation", target.generationID, "ownership_epoch", owner.Epoch, "stage", stage, "code", "handover_failed", "duration_ms", time.Since(started).Milliseconds())
		}
	}()
	sourceManager, err := c.engineManagerClient(source)
	if err != nil {
		return err
	}
	targetManager, err := c.engineManagerClient(target)
	if err != nil {
		return err
	}
	sourceInventory, err := sourceManager.Inventory(ctx)
	if err != nil || sourceInventory.GenerationID != source.generationID || sourceInventory.InstanceID != source.instanceID || !inventoryHasOwner(sourceInventory, lease.RecordingID, owner) {
		return errors.New("source Engine ownership inventory could not be confirmed")
	}
	targetInventory, err := targetManager.Inventory(ctx)
	if err != nil || targetInventory.GenerationID != target.generationID || targetInventory.InstanceID != target.instanceID || inventoryHasRecording(targetInventory, lease.RecordingID) {
		return errors.New("target Engine readiness inventory could not be confirmed")
	}

	snapshot, err := sourceManager.HandoverSnapshot(ctx, lease.RecordingID, owner)
	if err != nil || snapshot.Owner != owner || snapshot.RecordingID != lease.RecordingID {
		return errors.New("source Engine continuation snapshot could not be confirmed")
	}
	targetIdentity := acquire.HandoverTargetIdentity{EngineGeneration: target.generationID, WorkerInstance: target.instanceID}
	stage = "prepare_target"
	slog.Info("recording handover_prepare_started", "recording_id", lease.RecordingID, "source_generation", source.generationID, "target_generation", target.generationID, "ownership_epoch", owner.Epoch)
	if err := runtimehook.Pause(runtimehook.BeforeTargetPrepare, lease.RecordingID); err != nil {
		return errors.New("target preparation failpoint failed")
	}
	paused := false
	committed := false
	pausedSnapshot := snapshot
	defer func() {
		if !committed {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = targetManager.DiscardPreparedHandover(cleanupCtx, lease.RecordingID, targetIdentity)
			if paused {
				current, currentErr := c.ownerAuthority.CurrentOwner(lease.RecordingID)
				if currentErr == nil && current.EngineGeneration == source.generationID && current.WorkerInstance == source.instanceID {
					_ = sourceManager.ResumeHandover(cleanupCtx, lease.RecordingID, current, pausedSnapshot)
				}
			}
		}
	}()

	drainSource := func() (acquire.HandoverSnapshot, error) {
		drainStarted := time.Now()
		slog.Info("recording handover_source_drain_started", "recording_id", lease.RecordingID, "source_generation", source.generationID, "target_generation", target.generationID, "ownership_epoch", owner.Epoch, "stage", "source_drain")
		pauseCtx, cancelPause := context.WithTimeout(ctx, c.operationTimeout)
		defer cancelPause()
		finalSnapshot, pauseErr := sourceManager.PauseForHandover(pauseCtx, lease.RecordingID, owner)
		if pauseErr != nil || finalSnapshot.Owner != owner {
			return acquire.HandoverSnapshot{}, errors.New("source Engine could not reach a drained handover boundary")
		}
		paused = true
		pausedSnapshot = finalSnapshot
		slog.Info("recording handover_source_drained", "recording_id", lease.RecordingID, "source_generation", source.generationID, "target_generation", target.generationID, "ownership_epoch", owner.Epoch, "stage", "source_drain", "duration_ms", time.Since(drainStarted).Milliseconds())
		if err := runtimehook.Pause(runtimehook.AfterSourceDrain, lease.RecordingID); err != nil {
			return acquire.HandoverSnapshot{}, errors.New("source drain failpoint failed")
		}
		return finalSnapshot, nil
	}
	preparedErr := targetManager.PrepareHandoverTarget(ctx, snapshot, targetIdentity)
	var finalSnapshot acquire.HandoverSnapshot
	if errors.Is(preparedErr, acquire.ErrHandoverSourceRefreshRequired) || errors.Is(preparedErr, acquire.ErrHandoverSourceBoundaryRequired) {
		// Adapter Protocol v1 does not guarantee refresh idempotence, and a live
		// manifest at the already-committed tail cannot prove continuation. Drain
		// the source first, then repeat the complete target readiness proof.
		stage = "source_drain"
		finalSnapshot, err = drainSource()
		if err != nil {
			return err
		}
		stage = "prepare_drained_target"
		if err := targetManager.DiscardPreparedHandover(ctx, lease.RecordingID, targetIdentity); err != nil {
			return err
		}
		if err := targetManager.PrepareHandoverTargetAfterSourceDrain(ctx, finalSnapshot, targetIdentity); err != nil {
			return err
		}
	} else {
		if preparedErr != nil {
			return preparedErr
		}
		// Keep the usual optimistic path: the target prepares while the source
		// remains live. Then drain, discard the stale speculative payload and
		// prove continuation again from the final canonical boundary.
		stage = "source_drain"
		finalSnapshot, err = drainSource()
		if err != nil {
			return err
		}
		stage = "prepare_drained_target"
		if err := targetManager.DiscardPreparedHandover(ctx, lease.RecordingID, targetIdentity); err != nil {
			return err
		}
		if err := targetManager.PrepareHandoverTargetAfterSourceDrain(ctx, finalSnapshot, targetIdentity); err != nil {
			return err
		}
	}
	stage = "commit_owner"
	slog.Info("recording handover_target_ready", "recording_id", lease.RecordingID, "source_generation", source.generationID, "target_generation", target.generationID, "ownership_epoch", owner.Epoch, "duration_ms", time.Since(started).Milliseconds())
	if err := runtimehook.Pause(runtimehook.AfterTargetReady, lease.RecordingID); err != nil {
		return errors.New("target readiness failpoint failed")
	}
	if err := runtimehook.Pause(runtimehook.BeforeOwnerCAS, lease.RecordingID); err != nil {
		return errors.New("owner transfer failpoint failed")
	}

	targetOwner, transferErr := c.ownerAuthority.Transfer(owner, target.resourceOwnerID)
	if transferErr != nil {
		// Transfer can report a projection failure after the durable owner file
		// changed. The in-memory Host authority is consulted before any source
		// resume; a stale token is never reused.
		current, currentErr := c.ownerAuthority.CurrentOwner(lease.RecordingID)
		if currentErr == nil && current.EngineGeneration == source.generationID && current.WorkerInstance == source.instanceID {
			if current != owner {
				owner = current
			}
		}
		return transferErr
	}
	committed = true
	if err := runtimehook.Pause(runtimehook.BeforeTargetActivation, lease.RecordingID); err != nil {
		return errors.New("target activation failpoint failed after durable ownership transfer")
	}
	// Once the durable owner changes, finish or reverse that transaction even
	// if the periodic reconciliation context is canceled during shutdown.
	postCommitCtx, cancelPostCommit := context.WithTimeout(context.Background(), c.operationTimeout)
	defer cancelPostCommit()
	stage = "activate_target"
	activateErr := targetManager.ActivatePreparedHandover(postCommitCtx, targetOwner, targetIdentity)
	if activateErr != nil {
		// An IPC timeout is ambiguous. First fence the target back to the paused
		// source with a newer epoch. The shared canonical commit lock makes this
		// wait for any target write already in progress and reject all future ones.
		stage = "rollback_owner"
		restoredOwner, rollbackErr := c.ownerAuthority.RollbackTransfer(targetOwner, source.resourceOwnerID)
		if rollbackErr != nil {
			return errors.New("target activation was ambiguous and source recovery could not be proven")
		}
		_ = targetManager.CompleteHandover(postCommitCtx, lease.RecordingID, targetOwner)
		if err := sourceManager.ResumeHandover(postCommitCtx, lease.RecordingID, restoredOwner, finalSnapshot); err != nil {
			return errors.New("target was fenced but paused source could not resume")
		}
		committed = false
		paused = false
		stage = "aborted"
		slog.Info("recording handover_aborted", "recording_id", lease.RecordingID, "source_generation", source.generationID, "target_generation", target.generationID, "ownership_epoch", restoredOwner.Epoch, "stage", stage, "duration_ms", time.Since(started).Milliseconds())
		return errors.New("target activation failed; source resumed after a higher-epoch rollback")
	}
	stage = "complete_source"
	if err := runtimehook.Pause(runtimehook.BeforeSourceRetirement, lease.RecordingID); err != nil {
		return errors.New("source retirement failpoint failed after target activation")
	}
	if err := sourceManager.CompleteHandover(postCommitCtx, lease.RecordingID, owner); err != nil {
		// The target has the durable owner and source's old token remains fenced.
		// The next inventory pass must confirm the target before retirement.
		return errors.New("target owns the Recording but source detachment is incomplete")
	}
	committed = true
	stage = "complete"
	slog.Info("recording handover_committed", "recording_id", lease.RecordingID, "source_generation", source.generationID, "target_generation", target.generationID, "ownership_epoch", targetOwner.Epoch, "duration_ms", time.Since(started).Milliseconds())
	return nil
}

func (c *updateController) engineManagerClient(attachment engineAttachment) (*recorderengine.ManagerClient, error) {
	secret, err := controlplane.LoadPrivateIPCSecret(attachment.tokenPath)
	if err != nil {
		return nil, errors.New("Recorder Engine credential is unavailable")
	}
	client, err := runtimeipc.NewClientForInstance(attachment.socketPath, attachment.generationID, attachment.instanceID, secret, 3*time.Second)
	if err != nil {
		return nil, errors.New("Recorder Engine IPC endpoint is unavailable")
	}
	manager, err := recorderengine.NewManagerClient(client)
	if err != nil {
		return nil, errors.New("Recorder Engine manager client is unavailable")
	}
	return manager, nil
}

func inventoryHasOwner(inventory recorderengine.InventoryResult, recordingID string, owner recordingowner.Owner) bool {
	for _, row := range inventory.Active {
		if row.RecordingID == recordingID {
			return row.Owner != nil && *row.Owner == owner
		}
	}
	return false
}

func inventoryHasRecording(inventory recorderengine.InventoryResult, recordingID string) bool {
	for _, row := range inventory.Active {
		if row.RecordingID == recordingID {
			return true
		}
	}
	return false
}

func (c *updateController) retireSafeGenerations(ctx context.Context) error {
	if ctx == nil {
		return errors.New("generation retirement context is required")
	}
	snapshot := c.registry.Snapshot()
	idList := make([]string, 0, len(snapshot.Generations))
	for id := range snapshot.Generations {
		idList = append(idList, id)
	}
	sort.Strings(idList)
	for _, id := range idList {
		if err := ctx.Err(); err != nil {
			return err
		}
		current := c.registry.Snapshot()
		g, exists := current.Generations[id]
		rollbackTarget := id == current.PreviousGenerationID
		if !exists || (generationProtected(current, id) && !rollbackTarget) || generationHasLease(current, id) {
			continue
		}
		if rollbackTarget {
			// Keep the previous signed release and registry entry for rollback,
			// but do not keep an idle Engine process alive indefinitely. The
			// Engine is detached and stopped only after the fresh inventory pass
			// above has confirmed that it owns no Recording leases.
			if g.State != generation.StateDraining || g.EngineDormant {
				continue
			}
			if c.proveAndStopEngine(ctx, id, true) && c.processesStopped(id) {
				_ = c.registry.MarkEngineDormant(id)
			}
			continue
		}
		if g.State == generation.StateRetired {
			c.finishRetiredGeneration(ctx, g)
			continue
		}
		if g.State != generation.StateDraining && g.State != generation.StateFailed {
			continue
		}
		if !c.proveAndRetireEngine(ctx, id) {
			continue
		}
		if !c.processesStopped(id) {
			continue
		}
		if err := c.registry.Retire(id); err != nil {
			continue
		}
		retired := c.registry.Snapshot().Generations[id]
		c.finishRetiredGeneration(ctx, retired)
	}
	return nil
}

func generationProtected(snapshot generation.Snapshot, id string) bool {
	return id == snapshot.ActiveGenerationID || id == snapshot.PreviousGenerationID || id == snapshot.ActivationPreviousGenerationID || id == snapshot.StagedGenerationID
}

func generationHasLease(snapshot generation.Snapshot, id string) bool {
	for _, lease := range snapshot.Leases {
		if lease.EngineGeneration == id {
			return true
		}
	}
	return false
}

func (c *updateController) proveAndRetireEngine(ctx context.Context, id string) bool {
	return c.proveAndStopEngine(ctx, id, false)
}

func (c *updateController) proveAndStopEngine(ctx context.Context, id string, retainPrevious bool) bool {
	snapshot := c.supervisor.Snapshot()
	process, found := supervisorGeneration(snapshot, id)
	if found && (process.Engine.State == supervisor.ProcessStarting || process.Engine.State == supervisor.ProcessStopping) {
		return false
	}
	engineRunning := found && process.Engine.State == supervisor.ProcessReady
	if engineRunning {
		if err := c.drain.BeginDrain(ctx, id); err != nil {
			return false
		}
		active, err := c.drain.ActiveRecordings(ctx, id)
		if err != nil || active != 0 {
			return false
		}
	}
	latest := c.registry.Snapshot()
	protected := id == latest.ActiveGenerationID || id == latest.ActivationPreviousGenerationID || id == latest.StagedGenerationID
	if id == latest.PreviousGenerationID && !retainPrevious {
		protected = true
	}
	if retainPrevious && id != latest.PreviousGenerationID {
		protected = true
	}
	if protected || generationHasLease(latest, id) {
		return false
	}
	controlID, mayContain := activeControlCatalog(snapshot)
	if mayContain {
		if controlID == "" || c.engineDetacher == nil || c.engineDetacher.DetachEngine(ctx, controlID, id) != nil {
			return false
		}
	}
	if engineRunning {
		if err := c.supervisor.RetireEngine(ctx, id, c.drain); err != nil {
			return false
		}
	}
	return c.processesStopped(id)
}

func supervisorGeneration(snapshot supervisor.Snapshot, id string) (supervisor.GenerationSnapshot, bool) {
	for _, process := range snapshot.Generations {
		if process.ID == id {
			return process, true
		}
	}
	return supervisor.GenerationSnapshot{}, false
}

func activeControlCatalog(snapshot supervisor.Snapshot) (string, bool) {
	if snapshot.ActiveControlGeneration == "" {
		return "", false
	}
	process, exists := supervisorGeneration(snapshot, snapshot.ActiveControlGeneration)
	if !exists || !process.ControlActive || process.Control.State != supervisor.ProcessReady {
		// The active-control pointer is inconsistent with the process view, so
		// do not assume its Engine catalog is empty.
		return "", true
	}
	return snapshot.ActiveControlGeneration, true
}

func (c *updateController) processesStopped(id string) bool {
	process, exists := supervisorGeneration(c.supervisor.Snapshot(), id)
	if !exists {
		return true
	}
	return stoppedProcessState(process.Engine.State) && stoppedProcessState(process.Control.State) && !process.ControlActive
}

func stoppedProcessState(state supervisor.ProcessState) bool {
	return state == "" || state == supervisor.ProcessExited || state == supervisor.ProcessFailed
}

func (c *updateController) finishRetiredGeneration(ctx context.Context, g generation.Generation) {
	if ctx.Err() != nil {
		return
	}
	snapshot := c.registry.Snapshot()
	current, exists := snapshot.Generations[g.ID]
	if !exists || current.State != generation.StateRetired || generationProtected(snapshot, g.ID) || generationHasLease(snapshot, g.ID) || !c.processesStopped(g.ID) {
		return
	}
	if !c.isBundledGeneration(current) {
		dirID := install.ReleaseDirectoryID(current.Version, current.Commit)
		err := install.RemoveInstalledRelease(filepath.Join(c.config.DataDir, "runtime"), dirID, c.trustedKeys)
		if err != nil && !errors.Is(err, install.ErrReleaseNotFound) {
			return
		}
	}
	snapshot = c.registry.Snapshot()
	if current, exists = snapshot.Generations[g.ID]; !exists || current.State != generation.StateRetired || generationProtected(snapshot, g.ID) || generationHasLease(snapshot, g.ID) || !c.processesStopped(g.ID) {
		return
	}
	if err := c.registry.RemoveRetired(g.ID); err != nil {
		return
	}
	c.mu.Lock()
	delete(c.engines, g.ID)
	delete(c.manifests, g.ID)
	delete(c.releaseNotes, g.ID)
	delete(c.buildIdentities, g.ID)
	c.mu.Unlock()
	c.collectAdapterSets()
}

func (c *updateController) collectAdapterSets() {
	if c.registry == nil || c.supervisor == nil {
		return
	}
	registryState := c.registry.Snapshot()
	attachments := c.EngineAttachments()
	processes := c.supervisor.Snapshot()
	keep, consistent := adapterSetCollectionRoots(registryState, attachments, processes)
	if !consistent {
		// The registry and live process views no longer identify every possible
		// reader. Retain all immutable adapter and storage sets until a later
		// consistent pass.
		return
	}
	if c.adapterCatalog != nil {
		_ = c.adapterCatalog.Collect(keep)
	}
	if c.storageCatalog != nil {
		storageRoots, ok := storageProviderSetCollectionRoots(registryState, attachments, processes)
		if !ok {
			return
		}
		_ = c.storageCatalog.CollectGarbage(storageRoots)
	}
}

// storageProviderSetCollectionRoots mirrors the full generation/process
// consistency audit used for source adapter sets. A provider set remains
// pinned by every active, staged, draining, lease-bearing, or attached Engine
// generation. Ambiguous process state disables collection.
func storageProviderSetCollectionRoots(registryState generation.Snapshot, attachments []engineAttachment, processes supervisor.Snapshot) ([]string, bool) {
	if _, consistent := adapterSetCollectionRoots(registryState, attachments, processes); !consistent {
		return nil, false
	}
	roots := make(map[string]struct{})
	for id, item := range registryState.Generations {
		if item.ID != id || (item.StorageProviderSetID != "" && !validStorageProviderSetIdentity(item.StorageProviderSetID)) {
			return nil, false
		}
		switch item.State {
		case generation.StateActive, generation.StateDraining, generation.StateStaging, generation.StateVerified, generation.StateReady:
			if item.StorageProviderSetID != "" {
				roots[item.StorageProviderSetID] = struct{}{}
			}
		}
	}
	for _, lease := range registryState.Leases {
		item, exists := registryState.Generations[lease.EngineGeneration]
		if !exists || item.ID != lease.EngineGeneration {
			return nil, false
		}
		if item.StorageProviderSetID != "" {
			roots[item.StorageProviderSetID] = struct{}{}
		}
	}
	for _, attachment := range attachments {
		item, exists := registryState.Generations[attachment.generationID]
		if !exists || item.ID != attachment.generationID || item.StorageProviderSetID != attachment.storageProviderSetID {
			return nil, false
		}
		if attachment.storageProviderSetID != "" {
			roots[attachment.storageProviderSetID] = struct{}{}
		}
	}
	result := make([]string, 0, len(roots))
	for id := range roots {
		result = append(result, id)
	}
	sort.Strings(result)
	return result, true
}

// adapterSetCollectionRoots returns only adapter sets referenced by live
// generation state, recording leases, or Host Engine attachments. An
// inconsistent registry/process view disables collection entirely so an
// unknown Engine can never lose the artifact it may need to restart.
func adapterSetCollectionRoots(registryState generation.Snapshot, attachments []engineAttachment, processes supervisor.Snapshot) ([]string, bool) {
	if registryState.Generations == nil || registryState.Leases == nil {
		return nil, false
	}
	setRoots := make(map[string]struct{})
	addSet := func(setID string) bool {
		if setID == "" {
			return true
		}
		if !validAdapterSetIdentity(setID) {
			return false
		}
		setRoots[setID] = struct{}{}
		return true
	}
	lookup := func(id string) (generation.Generation, bool) {
		item, exists := registryState.Generations[id]
		return item, exists && item.ID == id && generationPattern.MatchString(id)
	}
	checkPointer := func(id string, expected generation.State) bool {
		if id == "" {
			return true
		}
		item, exists := lookup(id)
		return exists && item.State == expected
	}
	if !checkPointer(registryState.ActiveGenerationID, generation.StateActive) ||
		!checkPointer(registryState.PreviousGenerationID, generation.StateDraining) ||
		!checkPointer(registryState.ActivationPreviousGenerationID, generation.StateDraining) {
		return nil, false
	}
	if registryState.StagedGenerationID != "" {
		staged, exists := lookup(registryState.StagedGenerationID)
		if !exists || (staged.State != generation.StateStaging && staged.State != generation.StateVerified && staged.State != generation.StateReady) {
			return nil, false
		}
	}

	rootGenerations := make(map[string]struct{})
	activeCount := 0
	for id, item := range registryState.Generations {
		if item.ID != id || !generationPattern.MatchString(id) || (item.AdapterSetID != "" && !validAdapterSetIdentity(item.AdapterSetID)) || (item.StorageProviderSetID != "" && !validStorageProviderSetIdentity(item.StorageProviderSetID)) {
			return nil, false
		}
		switch item.State {
		case generation.StateActive:
			activeCount++
			if id != registryState.ActiveGenerationID {
				return nil, false
			}
			rootGenerations[id] = struct{}{}
		case generation.StateDraining:
			rootGenerations[id] = struct{}{}
		case generation.StateStaging, generation.StateVerified, generation.StateReady:
			// A candidate in any pre-activation state must remain rooted. The
			// registry state machine permits exactly the staged pointer to own it.
			if id != registryState.StagedGenerationID {
				return nil, false
			}
			rootGenerations[id] = struct{}{}
		case generation.StateFailed, generation.StateRetired:
			// These records are collectible unless a lease or Engine attachment
			// below still identifies a consumer.
		default:
			return nil, false
		}
	}
	if (registryState.ActiveGenerationID == "") != (activeCount == 0) || activeCount > 1 {
		return nil, false
	}
	for recordingID, lease := range registryState.Leases {
		item, exists := lookup(lease.EngineGeneration)
		if recordingID == "" || recordingID != lease.RecordingID || !exists || (item.State != generation.StateActive && item.State != generation.StateDraining) || item.EngineDormant {
			return nil, false
		}
		rootGenerations[lease.EngineGeneration] = struct{}{}
	}
	for id := range rootGenerations {
		if !addSet(registryState.Generations[id].AdapterSetID) {
			return nil, false
		}
	}

	attachmentByGeneration := make(map[string]engineAttachment, len(attachments))
	for _, attachment := range attachments {
		item, exists := lookup(attachment.generationID)
		if !exists || item.AdapterSetID != attachment.adapterSetID || item.StorageProviderSetID != attachment.storageProviderSetID || !addSet(attachment.adapterSetID) {
			return nil, false
		}
		if _, duplicate := attachmentByGeneration[attachment.generationID]; duplicate {
			return nil, false
		}
		attachmentByGeneration[attachment.generationID] = attachment
	}
	seenProcesses := make(map[string]struct{}, len(processes.Generations))
	for _, process := range processes.Generations {
		if !generationPattern.MatchString(process.ID) {
			return nil, false
		}
		if _, duplicate := seenProcesses[process.ID]; duplicate {
			return nil, false
		}
		seenProcesses[process.ID] = struct{}{}
		item, exists := lookup(process.ID)
		if !exists {
			return nil, false
		}
		if !stoppedProcessState(process.Engine.State) {
			if _, attached := attachmentByGeneration[process.ID]; !attached || item.EngineDormant || item.State == generation.StateFailed || item.State == generation.StateRetired {
				return nil, false
			}
		}
	}
	if processes.ActiveControlGeneration != "" {
		activeControl, exists := lookup(processes.ActiveControlGeneration)
		if !exists || activeControl.State != generation.StateActive {
			return nil, false
		}
		found := false
		for _, process := range processes.Generations {
			if process.ID == processes.ActiveControlGeneration && process.ControlActive && process.Control.State == supervisor.ProcessReady {
				found = true
				break
			}
		}
		if !found {
			return nil, false
		}
	}

	keep := make([]string, 0, len(setRoots))
	for setID := range setRoots {
		keep = append(keep, setID)
	}
	sort.Strings(keep)
	return keep, true
}

func validAdapterSetIdentity(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func validStorageProviderSetIdentity(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func (c *updateController) isBundledGeneration(g generation.Generation) bool {
	if attachment, ok := c.engineAttachment(g.ID); ok && attachment.releaseDir == c.config.BundleDir {
		return true
	}
	return matchesBuild(g, c.bundleBuild) || matchesBuild(g, c.hostBuild)
}

// EngineAttachments returns a stable copy of Host-local IPC attachments. The
// values include private socket and credential paths and are for bootstrap
// orchestration only, never public API projection.
func (c *updateController) EngineAttachments() []engineAttachment {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]engineAttachment, 0, len(c.engines))
	for _, attachment := range c.engines {
		copyAttachment := attachment
		if attachment.manifest != nil {
			manifest := *attachment.manifest
			copyAttachment.manifest = &manifest
		}
		result = append(result, copyAttachment)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].generationID < result[j].generationID })
	return result
}

func (c *updateController) fetchCandidate(ctx context.Context) (release.Manifest, install.Source, error) {
	if err := ctx.Err(); err != nil {
		return release.Manifest{}, nil, httpapi.NewControllerError("update_check_failed")
	}
	src, err := c.sourceFactory()
	if err != nil || src == nil {
		return release.Manifest{}, nil, c.fail("update_check_failed")
	}
	checkCtx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()
	manifestBytes, signature, err := src.Manifest(checkCtx)
	if err != nil {
		return release.Manifest{}, nil, c.fail("update_check_failed")
	}
	manifest, err := release.DecodeAndVerifyManifest(manifestBytes, signature, c.trustedKeys)
	if err != nil {
		return release.Manifest{}, nil, c.mapManifestError(err)
	}
	if err := release.CheckCompatibility(manifest, c.compatibility); err != nil {
		return release.Manifest{}, nil, c.mapManifestError(err)
	}
	if !c.archiveCompatibleWithRuntime(manifest) {
		return release.Manifest{}, nil, c.fail("release_incompatible")
	}
	c.mu.Lock()
	c.verification = "verified"
	c.mu.Unlock()
	return manifest, src, nil
}

func (c *updateController) availableForUpdates() error {
	c.mu.RLock()
	reason := c.unavailableReason
	c.mu.RUnlock()
	if reason != "" {
		return httpapi.NewControllerError("update_unavailable")
	}
	return nil
}

func (c *updateController) acquireOperation(ctx context.Context) error {
	if err := c.lockOperation(ctx, true); err != nil {
		return httpapi.NewControllerError("operation_conflict")
	}
	return nil
}

func (c *updateController) lockOperation(ctx context.Context, wait bool) error {
	if ctx == nil {
		return errors.New("runtime update operation context is required")
	}
	if wait {
		select {
		case c.gate <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case c.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return errors.New("runtime update operation is already running")
	}
}

func (c *updateController) releaseOperation() { <-c.gate }

func (c *updateController) cachedAvailable() (*release.Manifest, install.Source) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.available == nil {
		return nil, nil
	}
	manifest := *c.available
	return &manifest, c.availableSource
}

func (c *updateController) activeMatches(manifest release.Manifest) bool {
	snapshot := c.registry.Snapshot()
	active := snapshot.Generations[snapshot.ActiveGenerationID]
	return active.ID != "" && active.Version == manifest.ReleaseVersion && strings.EqualFold(active.Commit, manifest.Commit)
}

func (c *updateController) inspectGeneration(generationInfo generation.Generation) (*install.VerifiedRelease, error) {
	id := install.ReleaseDirectoryID(generationInfo.Version, generationInfo.Commit)
	installed, err := install.InspectInstalledRelease(filepath.Join(c.config.DataDir, "runtime"), id, c.trustedKeys, c.compatibility)
	if err != nil {
		return nil, err
	}
	if installed.Manifest.ReleaseVersion != generationInfo.Version || !strings.EqualFold(installed.Manifest.Commit, generationInfo.Commit) {
		return nil, release.ErrInvalidManifest
	}
	return &installed, nil
}

func (c *updateController) loadGeneration(generationInfo generation.Generation) (*install.VerifiedRelease, bool, error) {
	if attachment, ok := c.engineAttachment(generationInfo.ID); ok && attachment.releaseDir == c.config.BundleDir {
		return nil, true, nil
	}
	if matchesBuild(generationInfo, c.applicationBuild) || matchesBuild(generationInfo, c.hostBuild) {
		return nil, true, nil
	}
	installed, err := c.inspectGeneration(generationInfo)
	if err != nil {
		return nil, false, err
	}
	return installed, false, nil
}

func (c *updateController) startEngine(ctx context.Context, id, directory string, manifest release.Manifest, paths map[string]string) (engineAttachment, error) {
	if c.resourceSocket == "" || c.resourceTokenPath == "" {
		return engineAttachment{}, errors.New("Host resource coordinator IPC is unavailable")
	}
	if err := validateSocketPath(c.resourceSocket); err != nil {
		return engineAttachment{}, errors.New("Host resource coordinator socket is invalid")
	}
	if _, err := controlplane.LoadPrivateIPCSecret(c.resourceTokenPath); err != nil {
		return engineAttachment{}, errors.New("Host resource coordinator credential is unavailable")
	}
	executable := paths[release.RoleRecorderEngine]
	if executable == "" && directory == c.config.BundleDir {
		executable = filepath.Join(directory, "recorder-engine")
	}
	if executable == "" || filepath.Dir(executable) != directory {
		return engineAttachment{}, errors.New("verified Recorder Engine executable is unavailable")
	}
	if err := ensurePrivateDirectory(filepath.Join(c.config.DataDir, "runtime", "ipc")); err != nil {
		return engineAttachment{}, err
	}
	secret, err := randomSecret()
	if err != nil {
		return engineAttachment{}, err
	}
	suffix := id[:12]
	socket := filepath.Join(c.config.DataDir, "runtime", "ipc", "e-"+suffix+".sock")
	tokenPath := filepath.Join(c.config.DataDir, "runtime", "ipc", "e-"+suffix+".token")
	if err := validateSocketPath(socket); err != nil {
		return engineAttachment{}, err
	}
	if err := writeAtomicPrivate(tokenPath, secret); err != nil {
		return engineAttachment{}, errors.New("generation IPC credential could not be secured")
	}
	ownerNonce, err := newGenerationID()
	if err != nil {
		return engineAttachment{}, errors.New("generation resource owner identity could not be created")
	}
	owner := "e" + id + "-" + ownerNonce
	engineInstanceID, err := newGenerationID()
	if err != nil {
		return engineAttachment{}, errors.New("Recorder Engine instance identity could not be created")
	}
	gen, ok := c.registry.Snapshot().Generations[id]
	if !ok {
		return engineAttachment{}, errors.New("Recorder Engine generation is unavailable")
	}
	adapterDir, err := c.adapterDirectory(gen.AdapterSetID)
	if err != nil {
		return engineAttachment{}, err
	}
	env := commonChildEnv(c.config, adapterDir)
	env = append(env, "RUNTIME_RESOURCE_SOCKET_PATH="+c.resourceSocket, "RUNTIME_RESOURCE_TOKEN_FILE="+c.resourceTokenPath, "RUNTIME_RESOURCE_OWNER="+owner)
	env = append(env, storageProviderChildEnv(c.config.DataDir, gen.StorageProviderSetID)...)
	env = append(env, "ENGINE_SOCKET_PATH="+socket, "ENGINE_GENERATION_ID="+id, "ENGINE_INSTANCE_ID="+engineInstanceID, "ENGINE_RECOVERY_MODE=fresh", "ENGINE_IPC_TOKEN_FILE="+tokenPath)
	env = append(env, runtimehook.ChildEnvironment()...)
	spec := supervisor.ProcessSpec{GenerationID: id, Role: supervisor.RoleEngine, Executable: executable, Dir: directory, Env: env}
	c.readiness.register(id, supervisor.RoleEngine, socket, tokenPath)
	if err := c.supervisor.StartEngine(ctx, spec); err != nil {
		return engineAttachment{}, err
	}
	ready, err := c.readiness.engineIdentity(id)
	if err != nil || !ready.Ready || ready.GenerationID != id || ready.ProtocolVersion != runtimeipc.ProtocolVersion ||
		(c.ownerAuthority != nil && ready.InstanceID != engineInstanceID) {
		return engineAttachment{}, errors.New("Recorder Engine readiness identity is invalid")
	}
	if c.ownerAuthority != nil {
		if err := c.ownerAuthority.RegisterEngine(owner, id, ready.InstanceID); err != nil {
			return engineAttachment{}, errors.New("Recorder Engine resource identity could not be registered")
		}
	}
	attachment := engineAttachment{generationID: id, resourceOwnerID: owner, releaseDir: directory, adapterSetID: gen.AdapterSetID, storageProviderSetID: gen.StorageProviderSetID, socketPath: socket, tokenPath: tokenPath, instanceID: ready.InstanceID}
	if manifest.ReleaseVersion != "" {
		copyManifest := manifest
		attachment.manifest = &copyManifest
	}
	return attachment, nil
}

func (c *updateController) registerEngine(id string, attachment engineAttachment) error {
	if err := c.drain.register(id, attachment.socketPath, attachment.tokenPath, attachment.instanceID); err != nil {
		return err
	}
	c.mu.Lock()
	c.engines[id] = attachment
	if attachment.manifest != nil {
		c.manifests[id] = *attachment.manifest
	}
	c.mu.Unlock()
	return nil
}

func (c *updateController) publishControl(ctx context.Context, id, directory string, manifest release.Manifest, paths map[string]string, activeEngineID string) error {
	executable := paths[release.RoleControlPlane]
	if executable == "" && directory == c.config.BundleDir {
		executable = filepath.Join(directory, "control-plane")
	}
	if executable == "" || filepath.Dir(executable) != directory {
		return errors.New("verified Control Plane executable is unavailable")
	}
	entries, err := c.catalogEntries(activeEngineID)
	if err != nil {
		return err
	}
	ipcDir := filepath.Join(c.config.DataDir, "runtime", "ipc")
	if err := ensurePrivateDirectory(ipcDir); err != nil {
		return err
	}
	secret, err := randomSecret()
	if err != nil {
		return err
	}
	suffix := id[:12]
	socket := filepath.Join(ipcDir, "c-"+suffix+".sock")
	tokenPath := filepath.Join(ipcDir, "c-"+suffix+".token")
	catalogPath := filepath.Join(ipcDir, "catalog-"+id+".json")
	if err := validateSocketPath(socket); err != nil {
		return err
	}
	if err := writeAtomicPrivate(tokenPath, secret); err != nil {
		return errors.New("generation IPC credential could not be secured")
	}
	if err := c.catalogWriter(catalogPath, controlplane.EngineCatalog{Version: 1, ActiveGenerationID: activeEngineID, Engines: entries}); err != nil {
		return errors.New("private Recorder Engine catalog could not be published")
	}
	controlAddr, target, err := c.controlTarget()
	if err != nil {
		return err
	}
	ownerNonce, err := newGenerationID()
	if err != nil {
		return errors.New("generation resource owner identity could not be created")
	}
	resourceOwner := "c" + id + "-" + ownerNonce
	gen, ok := c.registry.Snapshot().Generations[id]
	if !ok {
		return errors.New("Control Plane generation is unavailable")
	}
	adapterDir, err := c.adapterDirectory(gen.AdapterSetID)
	if err != nil {
		return err
	}
	adapterSet, err := c.adapterCatalog.Load(gen.AdapterSetID)
	if err != nil || adapterSet.ID != gen.AdapterSetID {
		return errors.New("generation adapter set identity is unavailable")
	}
	trustJSON, err := marshalAdapterTrust(adapterSet)
	if err != nil {
		return errors.New("generation adapter trust projection is unavailable")
	}
	env := append([]string(nil), commonChildEnv(c.config, adapterDir)...)
	env = append(env,
		"CONTROL_ADDR="+controlAddr,
		"CONTROL_GENERATION_ID="+id,
		"CONTROL_IPC_SOCKET_PATH="+socket,
		"CONTROL_IPC_TOKEN_FILE="+tokenPath,
		"ENGINE_CATALOG_FILE="+catalogPath,
		"ACTIVE_ENGINE_GENERATION="+activeEngineID,
		"COOKIE_SECURE="+boolEnv(c.config.ForceSecureCookies),
		"RUNTIME_RESOURCE_SOCKET_PATH="+c.resourceSocket,
		"RUNTIME_RESOURCE_TOKEN_FILE="+c.resourceTokenPath,
		"RUNTIME_RESOURCE_OWNER="+resourceOwner,
		"CONTROL_ADAPTER_TRUST_JSON="+trustJSON,
	)
	env = append(env, storageProviderChildEnv(c.config.DataDir, gen.StorageProviderSetID)...)
	if c.config.AuthDisabled {
		env = append(env, "AUTH_DISABLED=1")
	}
	spec := supervisor.ProcessSpec{GenerationID: id, Role: supervisor.RoleControl, Executable: executable, Dir: directory, Env: env}
	c.readiness.register(id, supervisor.RoleControl, socket, tokenPath)
	if err := c.supervisor.StartControl(ctx, spec, target); err != nil {
		return err
	}
	ready, err := c.readiness.controlIdentity(id)
	if err != nil || !ready.Ready || ready.Active || ready.State != controlplane.LifecyclePassive || ready.GenerationID != id || ready.InstanceID == "" {
		return errors.New("Control Plane readiness identity is invalid")
	}
	if err := c.lifecycle.register(id, socket, tokenPath, ready.InstanceID); err != nil {
		return errors.New("Control Plane lifecycle endpoint is unavailable")
	}
	if manifest.ReleaseVersion != "" {
		c.mu.Lock()
		c.manifests[id] = manifest
		c.mu.Unlock()
	}
	return nil
}

func (c *updateController) adapterDirectory(setID string) (string, error) {
	if c.adapterCatalog == nil {
		if setID != "" {
			return "", errors.New("immutable adapter catalog is unavailable")
		}
		return "", nil
	}
	if setID == "" {
		return "", errors.New("generation adapter set identity is unavailable")
	}
	snapshot, err := c.adapterCatalog.Load(setID)
	if err != nil || snapshot.ID != setID || !filepath.IsAbs(snapshot.Directory) || filepath.Clean(snapshot.Directory) != snapshot.Directory {
		return "", errors.New("immutable generation adapter set is unavailable")
	}
	return snapshot.Directory, nil
}

func (c *updateController) catalogEntries(activeEngineID string) ([]controlplane.EngineCatalogEntry, error) {
	if !generationPattern.MatchString(activeEngineID) {
		return nil, errors.New("active Recorder Engine identity is invalid")
	}
	snapshot := c.supervisor.Snapshot()
	ready := make(map[string]bool, len(snapshot.Generations))
	for _, process := range snapshot.Generations {
		ready[process.ID] = process.Engine.State == supervisor.ProcessReady
	}
	attachments := c.EngineAttachments()
	entries := make([]controlplane.EngineCatalogEntry, 0, len(attachments))
	foundActive := false
	for _, attachment := range attachments {
		if !ready[attachment.generationID] {
			continue
		}
		if attachment.generationID == activeEngineID {
			foundActive = true
		}
		entries = append(entries, controlplane.EngineCatalogEntry{
			GenerationID: attachment.generationID, SocketPath: attachment.socketPath,
			TokenFile: attachment.tokenPath, InstanceID: attachment.instanceID,
		})
	}
	if !foundActive {
		return nil, errors.New("active Recorder Engine is not attached")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].GenerationID < entries[j].GenerationID })
	if len(entries) == 0 || len(entries) > 32 {
		return nil, errors.New("Recorder Engine catalog is outside its limit")
	}
	return entries, nil
}

func (c *updateController) failCandidate(ctx context.Context, id string) {
	snapshot := c.registry.Snapshot()
	if snapshot.ActiveGenerationID == id || c.supervisor.Snapshot().ActiveControlGeneration == id {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.supervisor.StopControl(cleanupCtx, id)
	if _, exists := c.engineAttachment(id); exists {
		_ = c.supervisor.RetireEngine(cleanupCtx, id, c.drain)
		if c.supervisorEngineState(id) != supervisor.ProcessReady {
			c.mu.Lock()
			delete(c.engines, id)
			delete(c.manifests, id)
			delete(c.releaseNotes, id)
			c.mu.Unlock()
		}
	}
	snapshot = c.registry.Snapshot()
	if snapshot.StagedGenerationID == id {
		_ = c.registry.Fail(id)
	}
}

func (c *updateController) status() httpapi.Status {
	registryState := c.registry.Snapshot()
	supState := c.supervisor.Snapshot()
	c.mu.RLock()
	hostBuild := c.hostBuild
	appBuild := c.applicationBuild
	verification := c.verification
	lastFailure := c.lastFailureCode
	unavailable := c.unavailableReason
	available := c.available
	availableNotes := c.availableNotes
	manifestByID := make(map[string]release.Manifest, len(c.manifests))
	for id, manifest := range c.manifests {
		manifestByID[id] = manifest
	}
	releaseNotesByID := make(map[string]string, len(c.releaseNotes))
	for id, summary := range c.releaseNotes {
		releaseNotesByID[id] = summary
	}
	buildByID := make(map[string]buildinfo.Info, len(c.buildIdentities))
	for id, build := range c.buildIdentities {
		buildByID[id] = build
	}
	c.mu.RUnlock()
	if active, ok := registryState.Generations[registryState.ActiveGenerationID]; ok {
		if manifest, found := manifestByID[active.ID]; found {
			appBuild = buildInfoFromManifest(manifest)
		} else if build, found := buildByID[active.ID]; found {
			appBuild = build
		} else if active.Version != appBuild.Version || !strings.EqualFold(active.Commit, appBuild.Commit) {
			if installed, err := c.inspectGeneration(active); err == nil {
				appBuild = buildInfoFromManifest(installed.Manifest)
			}
		}
	}
	status := httpapi.Status{
		Host: toAPIIdentity(hostBuild), Application: toAPIIdentity(appBuild),
		ActiveGenerations: []httpapi.GenerationSummary{}, DrainingGenerations: []httpapi.GenerationSummary{},
		VerificationState: verification, LastFailureCode: lastFailure,
		UpdateUnavailableReason: unavailable,
	}
	leaseCount := make(map[string]int, len(registryState.Leases))
	for _, lease := range registryState.Leases {
		leaseCount[lease.EngineGeneration]++
	}
	generations := make([]generation.Generation, 0, len(registryState.Generations))
	for _, item := range registryState.Generations {
		generations = append(generations, item)
	}
	sort.Slice(generations, func(i, j int) bool {
		if generations[i].InstalledAt.Equal(generations[j].InstalledAt) {
			return generations[i].ID < generations[j].ID
		}
		return generations[i].InstalledAt.After(generations[j].InstalledAt)
	})
	for _, item := range generations {
		row := httpapi.GenerationSummary{ID: item.ID, Version: item.Version, Commit: item.Commit, InstalledAt: item.InstalledAt, State: string(item.State), ActiveRecordings: leaseCount[item.ID]}
		switch item.State {
		case generation.StateActive:
			status.ActiveGenerations = append(status.ActiveGenerations, row)
		case generation.StateDraining:
			status.DrainingGenerations = append(status.DrainingGenerations, row)
		}
	}
	if len(status.ActiveGenerations) > 64 {
		status.ActiveGenerations = status.ActiveGenerations[:64]
	}
	if len(status.DrainingGenerations) > 64 {
		status.DrainingGenerations = status.DrainingGenerations[:64]
	}
	if active, ok := registryState.Generations[supState.ActiveControlGeneration]; ok {
		row := summaryFor(active, leaseCount[active.ID])
		status.ActiveControl = &row
	}
	if active, ok := registryState.Generations[registryState.ActiveGenerationID]; ok {
		row := summaryFor(active, leaseCount[active.ID])
		status.DefaultEngine = &row
	}
	if staged, ok := registryState.Generations[registryState.StagedGenerationID]; ok {
		status.StagedRelease = c.releaseSummary(staged, manifestByID, releaseNotesByID)
	}
	if previous, ok := registryState.Generations[registryState.PreviousGenerationID]; ok {
		status.PreviousRelease = c.releaseSummary(previous, manifestByID, releaseNotesByID)
	}
	if available != nil {
		status.AvailableRelease = releaseSummaryFromManifest(*available, availableNotes)
		status.UpdatesAvailable = true
	}
	return status
}

func (c *updateController) releaseSummary(g generation.Generation, manifests map[string]release.Manifest, notes map[string]string) *httpapi.ReleaseSummary {
	if manifest, ok := manifests[g.ID]; ok {
		return releaseSummaryFromManifest(manifest, notes[g.ID])
	}
	c.mu.RLock()
	build, buildFound := c.buildIdentities[g.ID]
	applicationBuild := c.applicationBuild
	hostBuild := c.hostBuild
	c.mu.RUnlock()
	if buildFound {
		if summary := releaseSummaryFromBuild(build); summary != nil {
			return summary
		}
	}
	if g.Version == applicationBuild.Version && strings.EqualFold(g.Commit, applicationBuild.Commit) && applicationBuild.ReleaseChannel != "development" {
		return releaseSummaryFromBuild(applicationBuild)
	}
	if g.Version == hostBuild.Version && strings.EqualFold(g.Commit, hostBuild.Commit) && hostBuild.ReleaseChannel != "development" {
		return releaseSummaryFromBuild(hostBuild)
	}
	if installed, err := c.inspectGeneration(g); err == nil {
		return releaseSummaryFromManifest(installed.Manifest)
	}
	return nil
}

func matchesBuild(g generation.Generation, build buildinfo.Info) bool {
	return build.Version != "" && g.Version == build.Version && strings.EqualFold(g.Commit, build.Commit)
}

// archiveCompatibleWithRuntime applies the explicit coexistence contract to
// every active or draining generation. A candidate must read each existing
// writer epoch, and retained generations must be able to read objects the
// candidate may write so rollback does not strand archives created after the
// switch.
func (c *updateController) archiveCompatibleWithRuntime(candidate release.Manifest) bool {
	snapshot := c.registry.Snapshot()
	for _, current := range snapshot.Generations {
		if current.State != generation.StateActive && current.State != generation.StateDraining {
			continue
		}
		if candidate.ArchiveReadMinimum > current.ArchiveWriteEpoch || candidate.ArchiveReadMaximum < current.ArchiveWriteEpoch {
			return false
		}
		if current.ArchiveReadCompatibility.Minimum > candidate.ArchiveWriteEpoch || current.ArchiveReadCompatibility.Maximum < candidate.ArchiveWriteEpoch {
			return false
		}
	}
	return true
}

func summaryFor(item generation.Generation, recordings int) httpapi.GenerationSummary {
	return httpapi.GenerationSummary{ID: item.ID, Version: item.Version, Commit: item.Commit, InstalledAt: item.InstalledAt, State: string(item.State), ActiveRecordings: recordings}
}

func releaseSummaryFromManifest(manifest release.Manifest, notes ...string) *httpapi.ReleaseSummary {
	summary := &httpapi.ReleaseSummary{Version: manifest.ReleaseVersion, Commit: strings.ToLower(manifest.Commit), BuildTime: canonicalRFC3339(manifest.BuildTime), ReleaseChannel: manifest.Channel}
	if len(notes) > 0 {
		summary.NotesSummary = notes[0]
	}
	return summary
}

func releaseNotesSummary(source install.Source) string {
	provider, ok := source.(install.ReleaseNotesSummarySource)
	if !ok {
		return ""
	}
	return provider.ReleaseNotesSummary()
}

func releaseSummaryFromBuild(build buildinfo.Info) *httpapi.ReleaseSummary {
	if build.ReleaseChannel == "development" || build.Version == "dev" || build.Commit == "unknown" {
		return nil
	}
	return &httpapi.ReleaseSummary{Version: build.Version, Commit: build.Commit, BuildTime: build.BuildTime, ReleaseChannel: build.ReleaseChannel}
}

func canonicalRFC3339(value string) string {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return ""
	}
	return parsed.Format(time.RFC3339)
}

func toAPIIdentity(build buildinfo.Info) httpapi.BuildIdentity {
	if build.Version == "" {
		build = buildinfo.Current()
	}
	return httpapi.BuildIdentity{Version: build.Version, Commit: build.Commit, BuildTime: build.BuildTime, ReleaseChannel: build.ReleaseChannel, RuntimeProtocolVersion: build.RuntimeProtocolVersion}
}

func buildInfoFromManifest(manifest release.Manifest) buildinfo.Info {
	return buildinfo.Info{Version: manifest.ReleaseVersion, Commit: strings.ToLower(manifest.Commit), BuildTime: canonicalRFC3339(manifest.BuildTime), ReleaseChannel: manifest.Channel, RuntimeProtocolVersion: buildinfo.RuntimeProtocolVersion}
}

func (c *updateController) mapManifestError(err error) error {
	if errors.Is(err, release.ErrIncompatible) {
		return c.fail("release_incompatible")
	}
	return c.fail("verification_failed")
}

func (c *updateController) mapInstallError(err error, fallback string) error {
	if errors.Is(err, release.ErrIncompatible) {
		return c.fail("release_incompatible")
	}
	if errors.Is(err, release.ErrInvalidSignature) || errors.Is(err, release.ErrUnknownSigningKey) || errors.Is(err, release.ErrInvalidManifest) || errors.Is(err, release.ErrArtifactFile) || errors.Is(err, release.ErrInvalidArtifact) {
		return c.fail("verification_failed")
	}
	return c.fail(fallback)
}

func (c *updateController) mapRegistryError(err error, fallback string) error {
	if errors.Is(err, generation.ErrInvalidTransition) || errors.Is(err, generation.ErrGenerationExists) {
		return c.fail("operation_conflict")
	}
	return c.fail(fallback)
}

func (c *updateController) mapLaunchError(err error, fallback string) error {
	if errors.Is(err, supervisor.ErrCandidateNotReady) {
		return c.fail("candidate_not_ready")
	}
	return c.fail(fallback)
}

func (c *updateController) fail(code string) error {
	c.mu.Lock()
	c.lastFailureCode = code
	if code == "verification_failed" || code == "release_incompatible" {
		c.verification = "failed"
	}
	c.mu.Unlock()
	return httpapi.NewControllerError(code)
}

func (c *updateController) engineAttachment(id string) (engineAttachment, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	attachment, ok := c.engines[id]
	return attachment, ok
}

func (c *updateController) supervisorEngineState(id string) supervisor.ProcessState {
	for _, item := range c.supervisor.Snapshot().Generations {
		if item.ID == id {
			return item.Engine.State
		}
	}
	return supervisor.ProcessExited
}

func (c *updateController) bundleExecutables() (string, map[string]string) {
	directory := c.config.BundleDir
	return directory, map[string]string{
		release.RoleRecorderEngine: filepath.Join(directory, "recorder-engine"),
		release.RoleControlPlane:   filepath.Join(directory, "control-plane"),
	}
}
