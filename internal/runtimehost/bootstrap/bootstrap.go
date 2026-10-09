// Package bootstrap starts the image-bundled application generation under a
// stable Runtime Host listener and coordinates verified application release
// staging, activation, rollback, and draining.
package bootstrap

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/buildinfo"
	"github.com/integrated-recorder/core/internal/controlplane"
	"github.com/integrated-recorder/core/internal/plugintrust"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/runtimehook"
	"github.com/integrated-recorder/core/internal/runtimehost/adaptercatalog"
	"github.com/integrated-recorder/core/internal/runtimehost/generation"
	"github.com/integrated-recorder/core/internal/runtimehost/httpapi"
	"github.com/integrated-recorder/core/internal/runtimehost/install"
	"github.com/integrated-recorder/core/internal/runtimehost/installation"
	"github.com/integrated-recorder/core/internal/runtimehost/pluginregistry"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/runtimehost/resources"
	"github.com/integrated-recorder/core/internal/runtimehost/storagecatalog"
	"github.com/integrated-recorder/core/internal/runtimehost/supervisor"
	"github.com/integrated-recorder/core/internal/runtimeipc"
	"github.com/integrated-recorder/core/internal/systemsettings"
)

const (
	defaultDataDir           = "/data"
	defaultListenAddr        = ":8080"
	defaultAdapterDirs       = "/external-adapters"
	defaultPluginRegistryURL = pluginregistry.OfficialCatalogV3URL
	defaultControlWait       = 45 * time.Second
	controlPrepareWait       = 2 * time.Minute
	defaultCloseWait         = 20 * time.Second
	maximumTokenFileSize     = 32
	maxControlTrustBytes     = 64 << 10
)

// defaultBundleDir is a linker-overridable build input so process-level tests
// can exercise the production Host bootstrap against a private image-bundle
// fixture. Production builds retain the immutable image path below; it is not
// operator-configurable at runtime.
var defaultBundleDir = "/opt/integrated-recorder/initial"

// defaultStorageLocalBinary is the image-bundled Storage Provider Protocol
// implementation used as the mandatory local primary provider.
var defaultStorageLocalBinary = "/usr/local/lib/integrated-recorder/plugins/storage.local"

// defaultBundledHLSBinary is the only bundled source adapter executable. The
// Runtime Host imports this exact path with Host-assigned provenance; it never
// scans /adapters as a directory.
var defaultBundledHLSBinary = "/adapters/integrated-recorder-adapter-hls"

var generationPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

// Config contains trusted Runtime Host startup inputs. BundleDir is deliberately
// not read from the environment: production executables come from the image's
// immutable initial-release directory.
type Config struct {
	DataDir              string
	BundleDir            string
	StorageLocalBinary   string
	BundledHLSBinary     string
	ListenAddr           string
	AdapterDirs          string
	AllowOperatorPlugins bool
	FFmpegPath           string
	AuthDisabled         bool
	ForceSecureCookies   bool
	TrustedReleaseKeys   map[string]ed25519.PublicKey
	PluginRegistryURL    string
	// PluginRegistryHTTPClient is a deterministic test seam. Production hosts
	// leave it nil and use the bounded public HTTPS client.
	PluginRegistryHTTPClient *http.Client
	// ReleaseBundleDir optionally selects a local signed package as the update
	// source. This supports offline administration and deterministic host tests;
	// it is still verified against TrustedReleaseKeys before installation.
	ReleaseBundleDir string
	ControlReadyWait time.Duration
	ShutdownTimeout  time.Duration
}

func DefaultConfig() Config {
	return Config{
		DataDir: defaultDataDir, BundleDir: defaultBundleDir, StorageLocalBinary: defaultStorageLocalBinary, BundledHLSBinary: defaultBundledHLSBinary,
		ListenAddr: defaultListenAddr, AdapterDirs: defaultAdapterDirs, PluginRegistryURL: defaultPluginRegistryURL,
		ControlReadyWait: defaultControlWait, ShutdownTimeout: defaultCloseWait,
	}
}

// ConfigFromEnv reads only operator-facing non-secret values. Child processes
// receive a separate explicit environment allowlist assembled below.
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	if getenv == nil {
		return Config{}, errors.New("runtime host environment is unavailable")
	}
	c := DefaultConfig()
	if value := strings.TrimSpace(getenv("DATA_DIR")); value != "" {
		c.DataDir = value
	}
	if value := strings.TrimSpace(getenv("ADDR")); value != "" {
		c.ListenAddr = value
	}
	if value := strings.TrimSpace(getenv("ADAPTER_DIR")); value != "" {
		c.AdapterDirs = value
	}
	switch strings.TrimSpace(getenv("IR_ALLOW_OPERATOR_PLUGINS")) {
	case "", "0":
		c.AllowOperatorPlugins = false
	case "1":
		c.AllowOperatorPlugins = true
	default:
		return Config{}, errors.New("IR_ALLOW_OPERATOR_PLUGINS must be 1 when enabled")
	}
	c.FFmpegPath = strings.TrimSpace(getenv("FFMPEG_PATH"))
	if value := strings.TrimSpace(getenv("IR_STORAGE_LOCAL_PLUGIN")); value != "" {
		c.StorageLocalBinary = value
	}
	c.AuthDisabled = getenv("AUTH_DISABLED") == "1"
	c.ForceSecureCookies = getenv("COOKIE_SECURE") == "1"
	c.ReleaseBundleDir = strings.TrimSpace(getenv("IR_RELEASE_BUNDLE_DIR"))
	if value := strings.TrimSpace(getenv("IR_PLUGIN_REGISTRY_URL")); value != "" {
		c.PluginRegistryURL = value
	}
	trustedKeys, err := parseTrustedReleaseKeys(strings.TrimSpace(getenv("IR_RELEASE_TRUSTED_KEYS_JSON")))
	if err != nil {
		return Config{}, err
	}
	c.TrustedReleaseKeys = trustedKeys
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.DataDir) == "" || !filepath.IsAbs(c.DataDir) || filepath.Clean(c.DataDir) != c.DataDir || strings.ContainsRune(c.DataDir, 0) {
		return errors.New("DATA_DIR must be an absolute clean path")
	}
	if strings.TrimSpace(c.BundleDir) == "" || !filepath.IsAbs(c.BundleDir) || filepath.Clean(c.BundleDir) != c.BundleDir || strings.ContainsRune(c.BundleDir, 0) {
		return errors.New("bundled release directory must be an absolute clean path")
	}
	if strings.TrimSpace(c.StorageLocalBinary) == "" || !filepath.IsAbs(c.StorageLocalBinary) || filepath.Clean(c.StorageLocalBinary) != c.StorageLocalBinary || strings.ContainsRune(c.StorageLocalBinary, 0) {
		return errors.New("bundled storage.local executable path must be an absolute clean path")
	}
	if strings.TrimSpace(c.BundledHLSBinary) == "" || !filepath.IsAbs(c.BundledHLSBinary) || filepath.Clean(c.BundledHLSBinary) != c.BundledHLSBinary || strings.ContainsRune(c.BundledHLSBinary, 0) {
		return errors.New("bundled source.hls executable path must be an absolute clean path")
	}
	if strings.TrimSpace(c.AdapterDirs) == "" || strings.ContainsRune(c.AdapterDirs, 0) {
		return errors.New("adapter directories are invalid")
	}
	if err := validateOperatorAdapterDirs(c.AdapterDirs, c.BundledHLSBinary); err != nil {
		return err
	}
	if c.ReleaseBundleDir != "" && (!filepath.IsAbs(c.ReleaseBundleDir) || filepath.Clean(c.ReleaseBundleDir) != c.ReleaseBundleDir || strings.ContainsRune(c.ReleaseBundleDir, 0)) {
		return errors.New("IR_RELEASE_BUNDLE_DIR must be an absolute clean path")
	}
	if strings.ContainsRune(c.FFmpegPath, 0) {
		return errors.New("FFMPEG_PATH is invalid")
	}
	if len(c.TrustedReleaseKeys) > 16 {
		return errors.New("IR_RELEASE_TRUSTED_KEYS_JSON exceeds the supported key count")
	}
	for id, key := range c.TrustedReleaseKeys {
		if !releaseKeyIDPattern.MatchString(id) || len(key) != ed25519.PublicKeySize {
			return errors.New("IR_RELEASE_TRUSTED_KEYS_JSON contains an invalid key")
		}
	}
	if err := validateListenAddress(c.ListenAddr); err != nil {
		return err
	}
	if c.AuthDisabled && !listenIsLoopback(c.ListenAddr) {
		return errors.New("AUTH_DISABLED is allowed only when the Runtime Host listener binds to loopback")
	}
	if c.ControlReadyWait <= 0 || c.ControlReadyWait > 2*time.Minute || c.ShutdownTimeout <= 0 || c.ShutdownTimeout > 2*time.Minute {
		return errors.New("runtime startup and shutdown timeouts are invalid")
	}
	return nil
}

// Run bootstraps one image-bundled immutable generation, activates its Control
// process, and keeps the public listener and update API in the Runtime Host.
func Run(ctx context.Context, config Config) error {
	if ctx == nil {
		return errors.New("runtime host context is required")
	}
	if err := config.Validate(); err != nil {
		return err
	}
	if err := validateBundle(config.BundleDir); err != nil {
		return err
	}
	if err := makePrivateRuntimeDirs(config.DataDir); err != nil {
		return err
	}
	adminState := installation.AdminMissing
	if !config.AuthDisabled {
		configured, inspectErr := authn.InspectAdministrator(config.DataDir)
		if inspectErr != nil {
			adminState = installation.AdminUnknown
		} else if configured {
			adminState = installation.AdminConfigured
		}
	}
	installState, err := installation.Reconcile(config.DataDir, adminState, config.AuthDisabled)
	if err != nil {
		return fmt.Errorf("initialize Runtime Host installation state: %w", err)
	}
	var hostAuth *authn.Service
	if !config.AuthDisabled {
		if installState.Snapshot().State == installation.StateRecoveryRequired {
			hostAuth, err = authn.OpenWithoutBootstrap(config.DataDir)
		} else {
			hostAuth, err = authn.Open(config.DataDir)
		}
		if err != nil {
			return fmt.Errorf("initialize Runtime Host authentication: %w", err)
		}
	}
	if installState.Snapshot().State == installation.StateUninitialized || installState.Snapshot().State == installation.StateSetupInProgress {
		log.Print(firstRunSetupConsoleMessage(config.DataDir))
	} else if installState.Snapshot().State == installation.StateRecoveryRequired {
		log.Printf("Installation recovery is required (diagnostic: %s).", installState.Snapshot().DiagnosticCode)
	}
	listener, err := net.Listen("tcp", config.ListenAddr)
	if err != nil {
		return errors.New("Runtime Host listener could not be opened")
	}
	defer listener.Close()

	ipcDir := filepath.Join(config.DataDir, "runtime", "ipc")
	hostBootID, err := newGenerationID()
	if err != nil {
		return errors.New("Runtime Host boot identity could not be created")
	}
	stateDir := filepath.Join(config.DataDir, "runtime", "state")
	registry, err := generation.Open(filepath.Join(stateDir, "generations.json"))
	if err != nil {
		return fmt.Errorf("open generation registry: %w", err)
	}
	ownerStore, err := recordingowner.Open(config.DataDir)
	if err != nil {
		return fmt.Errorf("open recording owner store: %w", err)
	}
	ownerAuthority, err := resources.NewRecordingOwnerAuthority(ownerStore, registry)
	if err != nil {
		return fmt.Errorf("initialize recording owner authority: %w", err)
	}
	settings, err := systemsettings.Open(config.DataDir)
	if err != nil {
		return fmt.Errorf("open effective storage settings: %w", err)
	}
	ingest := settings.Current().Storage.IngestOptions()
	coordinator, err := resources.NewWithTelemetry(resources.Limits{
		GlobalBufferBytes: ingest.GlobalBytes, PerRecordingBufferBytes: ingest.PerRecordingBytes,
		QueueObjects: ingest.QueueObjects, WriterConcurrency: ingest.Writers,
	}, ingest.SampleInterval, ingest.MetricsRetention)
	if err != nil {
		return fmt.Errorf("configure process-wide ingest limits: %w", err)
	}
	defer coordinator.Close()
	resourceToken, err := randomSecret()
	if err != nil {
		return err
	}
	resourceSocket, resourceTokenPath, err := resourceIPCPaths(ipcDir, hostBootID)
	if err != nil {
		return err
	}
	if err := writeAtomicPrivate(resourceTokenPath, resourceToken); err != nil {
		return errors.New("runtime resource credential could not be secured")
	}
	resourceServer, err := resources.NewIPCServerWithRecordingOwners(resourceSocket, resourceToken, coordinator, ownerAuthority)
	if err != nil {
		return fmt.Errorf("create runtime resource IPC service: %w", err)
	}
	resourceCtx, cancelResources := context.WithCancel(context.Background())
	resourceDone := make(chan error, 1)
	go func() { resourceDone <- resourceServer.Serve(resourceCtx) }()
	defer func() {
		cancelResources()
		select {
		case <-resourceDone:
		case <-time.After(2 * time.Second):
		}
	}()

	launcher := supervisor.ExecLauncher{}
	readiness := newProcessReadiness(config.ControlReadyWait)
	lifecycle := newControlLifecycle()
	sup, err := supervisor.New(supervisor.Options{
		Launcher: launcher, Readiness: readiness, ControlLifecycle: lifecycle,
		OnProcessExit: func(spec supervisor.ProcessSpec, _ error) {
			releaseProcessResources(coordinator, spec)
			if spec.Role == supervisor.RoleEngine {
				_ = ownerAuthority.UnregisterEngine(
					childEnvValue(spec.Env, "RUNTIME_RESOURCE_OWNER"), spec.GenerationID,
					childEnvValue(spec.Env, "ENGINE_INSTANCE_ID"),
				)
			}
		},
		ShutdownTimeout: config.ShutdownTimeout, CleanupTimeout: 3 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("initialize Runtime Host process supervisor: %w", err)
	}
	supervisorClosed := false
	defer func() {
		if !supervisorClosed {
			closeCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
			defer cancel()
			_ = sup.Close(closeCtx)
		}
	}()

	build := buildinfo.Current()
	registrySnapshot := registry.Snapshot()
	if registrySnapshot.StagedGenerationID != "" {
		// The old Host process cannot have surviving managed child processes at
		// cold startup. Any persisted staged marker therefore represents a
		// candidate that never completed host activation.
		if err := registry.Fail(registrySnapshot.StagedGenerationID); err != nil {
			return fmt.Errorf("reconcile incomplete staged generation: %w", err)
		}
		registrySnapshot = registry.Snapshot()
	}
	selected, err := selectRuntimeRelease(registrySnapshot, build, config.BundleDir, filepath.Join(config.DataDir, "runtime"), config.TrustedReleaseKeys)
	if err != nil {
		return err
	}
	// Production imports are explicitly classified below. In particular, the
	// catalog does not scan /adapters or any mutable source directory by default.
	adapterCatalog, err := adaptercatalog.Open(filepath.Join(config.DataDir, "runtime", "adapters"), nil)
	if err != nil {
		return errors.New("Runtime Host adapter catalog is unavailable")
	}
	storageCatalog, err := storagecatalog.Open(filepath.Join(config.DataDir, "runtime", "storage-providers"))
	if err != nil {
		return errors.New("Runtime Host storage provider catalog is unavailable")
	}
	localStorageSet, err := ensureBundledLocalStorage(ctx, storageCatalog, config.StorageLocalBinary, filepath.Join(config.DataDir, "recordings"))
	if err != nil {
		return fmt.Errorf("initialize bundled storage.local provider: %w", err)
	}
	// Legacy generation records omitted storage-provider identity while the
	// Core wrote directly to /data/recordings. Adopt that exact physical archive
	// through the bundled provider without moving any objects. Persist the
	// immutable provider identity before application children can start.
	if err := registry.AdoptLegacyStorageProviderSet(localStorageSet.ID); err != nil {
		return fmt.Errorf("adopt legacy local archive into storage.local: %w", err)
	}
	registrySnapshot = registry.Snapshot()
	plugins, err := pluginregistry.Open(pluginregistry.Config{
		Root: filepath.Join(config.DataDir, "runtime", "plugin-registry"), RegistryURL: config.PluginRegistryURL,
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, HTTPClient: config.PluginRegistryHTTPClient,
	})
	if err != nil {
		return errors.New("Runtime Host plugin registry state is unavailable")
	}
	registrySources, err := plugins.DesiredSources()
	if err != nil {
		return errors.New("Runtime Host plugin source state is unavailable")
	}
	adapterSources, err := configuredAdapterSources(config, registrySources)
	if err != nil {
		return errors.New("Runtime Host adapter source configuration is invalid")
	}
	adapterSet, activeGeneration, activeExists, err := reconcileStartupAdapterSetClassified(ctx, adapterCatalog, registrySnapshot, adapterSources)
	if err != nil {
		return err
	}
	// A Host crash during an application activation leaves a durable journal.
	// First complete recovery using the journal's active tuple; a changed source
	// set is picked up by the normal periodic reconciliation after readiness.
	if registrySnapshot.ActivationPreviousGenerationID != "" && activeExists && adapterSet.ID != activeGeneration.AdapterSetID {
		adapterSet, err = adapterCatalog.Load(activeGeneration.AdapterSetID)
		if err != nil {
			return errors.New("active Runtime Host adapter set is unavailable")
		}
	}
	storageSetID, storageInstanceID, err := resolveStartupStoragePin(storageCatalog, registrySnapshot, localStorageSet.ID)
	if err != nil {
		return err
	}
	selected, startupGeneration, needsRegistryStage, err := prepareStartupGeneration(selected, registrySnapshot, adapterSet.ID, storageSetID, storageInstanceID)
	if err != nil {
		return err
	}
	if startupGeneration.StorageProviderSetID == "" {
		return errors.New("active Runtime Host storage provider set is unavailable")
	}
	if _, err := storageCatalog.LoadSet(startupGeneration.StorageProviderSetID); err != nil {
		return errors.New("active Runtime Host storage provider set is unavailable")
	}
	if err := probeStorageProviderSet(ctx, filepath.Join(config.DataDir, "runtime", "storage-providers"), startupGeneration.StorageProviderSetID); err != nil {
		return errors.New("active Runtime Host storage provider is unavailable")
	}
	generationID := selected.generationID
	engineToken, err := randomSecret()
	if err != nil {
		return err
	}
	controlToken, err := randomSecret()
	if err != nil {
		return err
	}
	engineSocket, engineTokenPath, controlSocket, controlTokenPath, err := generationIPCPaths(ipcDir, generationID, hostBootID)
	if err != nil {
		return err
	}
	catalogPath := filepath.Join(ipcDir, "engines.json")
	for path, token := range map[string][]byte{engineTokenPath: engineToken, controlTokenPath: controlToken} {
		if err := writeAtomicPrivate(path, token); err != nil {
			return errors.New("generation IPC credential could not be secured")
		}
	}
	ownerID, err := newResourceOwnerID(generationID)
	if err != nil {
		return err
	}
	engineInstanceID, err := newGenerationID()
	if err != nil {
		return errors.New("Recorder Engine instance identity could not be created")
	}
	resourceEnv := []string{
		"RUNTIME_RESOURCE_SOCKET_PATH=" + resourceSocket,
		"RUNTIME_RESOURCE_TOKEN_FILE=" + resourceTokenPath,
		"RUNTIME_RESOURCE_OWNER=" + ownerID,
	}
	commonEnv := commonChildEnv(config, adapterSet.Directory)
	engineRecoveryMode := initialEngineRecoveryMode(installState.Snapshot().State)
	engineEnv := append(append(append([]string(nil), commonEnv...), resourceEnv...),
		"ENGINE_SOCKET_PATH="+engineSocket,
		"ENGINE_GENERATION_ID="+generationID,
		"ENGINE_INSTANCE_ID="+engineInstanceID,
		"ENGINE_RECOVERY_MODE="+engineRecoveryMode,
		"ENGINE_IPC_TOKEN_FILE="+engineTokenPath)
	engineEnv = append(engineEnv, storageProviderChildEnv(config.DataDir, startupGeneration.StorageProviderSetID)...)
	engineEnv = append(engineEnv, runtimehook.ChildEnvironment()...)
	engineSpec := supervisor.ProcessSpec{
		GenerationID: generationID, Role: supervisor.RoleEngine,
		Executable: selected.enginePath, Dir: selected.directory,
		Env: engineEnv,
	}
	readiness.register(generationID, supervisor.RoleEngine, engineSocket, engineTokenPath)
	if needsRegistryStage {
		if err := registry.Stage(startupGeneration); err != nil {
			return fmt.Errorf("stage bundled generation: %w", err)
		}
		if err := registry.MarkVerified(generationID); err != nil {
			_ = registry.Fail(generationID)
			return fmt.Errorf("mark bundled generation verified: %w", err)
		}
	}
	startCtx, cancelStart := context.WithTimeout(ctx, config.ControlReadyWait)
	defer cancelStart()
	if err := sup.StartEngine(startCtx, engineSpec); err != nil {
		if needsRegistryStage {
			_ = registry.Fail(generationID)
		}
		return fmt.Errorf("start bundled Recorder Engine: %w", err)
	}
	engineReady, err := readiness.engineIdentity(generationID)
	if err != nil {
		return fmt.Errorf("confirm Recorder Engine identity: %w", err)
	}
	if engineReady.InstanceID != engineInstanceID {
		return errors.New("Recorder Engine readiness instance does not match Host launch identity")
	}
	if err := ownerAuthority.RegisterEngine(ownerID, generationID, engineReady.InstanceID); err != nil {
		return fmt.Errorf("register Recorder Engine recording authority: %w", err)
	}
	drain := newEngineDrain()
	if err := drain.register(generationID, engineSocket, engineTokenPath, engineReady.InstanceID); err != nil {
		return fmt.Errorf("attach Recorder Engine drain endpoint: %w", err)
	}
	catalog := controlplane.EngineCatalog{
		Version: 1, ActiveGenerationID: generationID,
		Engines: []controlplane.EngineCatalogEntry{{GenerationID: generationID, SocketPath: engineSocket, TokenFile: engineTokenPath, InstanceID: engineReady.InstanceID}},
	}
	if err := writeEngineCatalog(catalogPath, catalog); err != nil {
		return fmt.Errorf("publish private Recorder Engine catalog: %w", err)
	}
	controlAddr, controlTarget, err := privateControlTarget()
	if err != nil {
		return err
	}
	controlEnv := append([]string(nil), commonEnv...)
	controlOwnerID, err := newResourceOwnerIDForRole("c", generationID)
	if err != nil {
		return err
	}
	controlEnv = append(controlEnv,
		"RUNTIME_RESOURCE_SOCKET_PATH="+resourceSocket,
		"RUNTIME_RESOURCE_TOKEN_FILE="+resourceTokenPath,
		"RUNTIME_RESOURCE_OWNER="+controlOwnerID,
		"CONTROL_ADDR="+controlAddr,
		"CONTROL_GENERATION_ID="+generationID,
		"CONTROL_IPC_SOCKET_PATH="+controlSocket,
		"CONTROL_IPC_TOKEN_FILE="+controlTokenPath,
		"ENGINE_CATALOG_FILE="+catalogPath,
		"ACTIVE_ENGINE_GENERATION="+generationID,
		"COOKIE_SECURE="+boolEnv(config.ForceSecureCookies),
	)
	trustJSON, err := marshalAdapterTrust(adapterSet)
	if err != nil {
		return errors.New("generation adapter trust projection is unavailable")
	}
	controlEnv = append(controlEnv, "CONTROL_ADAPTER_TRUST_JSON="+trustJSON)
	controlEnv = append(controlEnv, storageProviderChildEnv(config.DataDir, startupGeneration.StorageProviderSetID)...)
	if config.AuthDisabled {
		controlEnv = append(controlEnv, "AUTH_DISABLED=1")
	}
	controlSpec := supervisor.ProcessSpec{
		GenerationID: generationID, Role: supervisor.RoleControl,
		Executable: selected.controlPath, Dir: selected.directory,
		Env: controlEnv,
	}
	readiness.register(generationID, supervisor.RoleControl, controlSocket, controlTokenPath)
	if err := sup.StartControl(startCtx, controlSpec, controlTarget); err != nil {
		if needsRegistryStage {
			_ = registry.Fail(generationID)
		}
		return fmt.Errorf("start bundled Control Plane: %w", err)
	}
	controlReady, err := readiness.controlIdentity(generationID)
	if err != nil {
		return fmt.Errorf("confirm Control Plane identity: %w", err)
	}
	if err := lifecycle.register(generationID, controlSocket, controlTokenPath, controlReady.InstanceID); err != nil {
		return fmt.Errorf("pin Control Plane lifecycle identity: %w", err)
	}
	if needsRegistryStage {
		if err := registry.MarkReady(generationID); err != nil {
			_ = registry.Fail(generationID)
			return fmt.Errorf("mark bundled generation ready: %w", err)
		}
		// Persist the candidate as the durable active generation while its
		// Control process is still passive. Cold recovery can then reconcile all
		// leases against this one authorized Engine generation before any
		// background producer is enabled.
		if err := registry.Activate(generationID); err != nil {
			_ = registry.Fail(generationID)
			return fmt.Errorf("persist bundled generation activation: %w", err)
		}
	}
	if err := reconcileColdGenerationState(registry, engineRecoveryMode, generationID, true); err != nil {
		return errors.New("recording generation leases could not be reconciled after cold recovery")
	}
	if err := sup.ActivateControl(startCtx, generationID); err != nil {
		return fmt.Errorf("activate bundled Control Plane: %w", err)
	}
	if registry.Snapshot().ActivationPreviousGenerationID != "" {
		if err := registry.FinalizeActivation(generationID); err != nil {
			return fmt.Errorf("finalize recovered generation activation: %w", err)
		}
	}
	installer, err := install.NewInstaller(filepath.Join(config.DataDir, "runtime"), config.TrustedReleaseKeys, currentHostCompatibility())
	if err != nil {
		return fmt.Errorf("initialize immutable release installer: %w", err)
	}
	initialEngine := engineAttachment{
		generationID: generationID, resourceOwnerID: ownerID, releaseDir: selected.directory, adapterSetID: adapterSet.ID, storageProviderSetID: startupGeneration.StorageProviderSetID, socketPath: engineSocket,
		tokenPath: engineTokenPath, instanceID: engineReady.InstanceID, manifest: selected.manifest,
	}
	updates, err := newUpdateController(updateControllerOptions{
		Config: config, HostBuild: build, ApplicationBuild: selected.appBuild,
		Registry: registry, AdapterCatalog: adapterCatalog, Installation: installState,
		PluginRegistry: plugins, StorageCatalog: storageCatalog,
		Supervisor: sup, Readiness: readiness, Lifecycle: lifecycle, Drain: drain,
		EngineDetacher: lifecycle,
		Coordinator:    coordinator, OwnerAuthority: ownerAuthority, Installer: installer, TrustedKeys: config.TrustedReleaseKeys,
		Compatibility: currentHostCompatibility(), SourceFactory: configuredReleaseSourceFactory(config, selected.appBuild),
		Engines: []engineAttachment{initialEngine}, ActiveControlID: generationID,
		ResourceSocket: resourceSocket, ResourceTokenPath: resourceTokenPath,
	})
	if err != nil {
		return fmt.Errorf("initialize Runtime Host update controller: %w", err)
	}
	updateHandler, err := httpapi.NewWithAudit(hostAuth, config.AuthDisabled, config.ForceSecureCookies, updates, sup, lifecycle)
	if err != nil {
		return fmt.Errorf("initialize Runtime Host update API: %w", err)
	}
	setupHandler, err := httpapi.NewWithSetup(hostAuth, config.AuthDisabled, installState, build, lifecycle, updateHandler)
	if err != nil {
		return fmt.Errorf("initialize Runtime Host setup API: %w", err)
	}
	if err := updates.ReconcileLeases(ctx); err != nil {
		return fmt.Errorf("confirm initial Recorder Engine inventory: %w", err)
	}
	leaseCtx, stopLeaseReconciliation := context.WithCancel(ctx)
	leaseDone := make(chan struct{})
	go func() {
		defer close(leaseDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
				// A lease pass may perform a bounded recording handover. Use the
				// same operation budget as update activation so a slow but healthy
				// source drain is not canceled and retried forever at 15 seconds.
				callCtx, cancel := context.WithTimeout(leaseCtx, updateOperationTimeout)
				_ = updates.ReconcileLeases(callCtx)
				cancel()
			}
		}
	}()
	adapterReconcileDone := startPeriodicAdapterReconciliation(leaseCtx, installState, updates, 3*time.Second)
	serveErr := sup.ServeWithHostHandler(ctx, listener, "/api", setupHandler)
	stopLeaseReconciliation()
	<-leaseDone
	<-adapterReconcileDone
	supervisorClosed = true
	return serveErr
}

// PrintSetupCode writes the already-created one-time setup credential to the
// caller's local terminal. It performs no mutation and refuses to expose a
// credential after administrator claim or installation completion.
func PrintSetupCode(dataDir string, output io.Writer) error {
	if output == nil || strings.TrimSpace(dataDir) == "" || !filepath.IsAbs(dataDir) || filepath.Clean(dataDir) != dataDir {
		return errors.New("setup code is unavailable")
	}
	snapshot := installation.ReadOnly(dataDir)
	if snapshot.State != installation.StateUninitialized && snapshot.State != installation.StateSetupInProgress {
		return errors.New("setup code is unavailable")
	}
	code, err := authn.ReadSetupCode(dataDir)
	if err != nil || code == "" || strings.ContainsAny(code, "\r\n") {
		return errors.New("setup code is unavailable")
	}
	// Bootstrap runs in another process. Recheck shared durable state before
	// writing any bytes so a setup code cannot be printed after a concurrent
	// administrator claim. Keep the error deliberately generic and path-free.
	configured, inspectErr := authn.InspectAdministrator(dataDir)
	if inspectErr != nil || configured {
		return errors.New("setup code is unavailable")
	}
	snapshot = installation.ReadOnly(dataDir)
	if snapshot.State != installation.StateUninitialized && snapshot.State != installation.StateSetupInProgress {
		return errors.New("setup code is unavailable")
	}
	if _, err := io.WriteString(output, code+"\n"); err != nil {
		return errors.New("setup code could not be written")
	}
	return nil
}

func firstRunSetupConsoleMessage(dataDir string) string {
	var output strings.Builder
	if err := PrintSetupCode(dataDir, &output); err != nil {
		return "First-run setup is required. Obtain the one-time setup code with `runtime-host setup-code` inside the runtime."
	}
	return fmt.Sprintf("First-run setup is required. Enter one-time setup code from local Runtime Host console/container logs: %s. CLI fallback: `runtime-host setup-code`.", strings.TrimSpace(output.String()))
}

func releaseProcessResources(coordinator *resources.Coordinator, spec supervisor.ProcessSpec) {
	if coordinator == nil || (spec.Role != supervisor.RoleEngine && spec.Role != supervisor.RoleControl) || !generationPattern.MatchString(spec.GenerationID) {
		return
	}
	// The supervisor invokes this only after the child is confirmed dead.
	// Resource identity is process-scoped, not generation-scoped: a restarted
	// process must not inherit or release another process instance's leases.
	ownerID := childEnvValue(spec.Env, "RUNTIME_RESOURCE_OWNER")
	if ownerID == "" || len(ownerID) > 128 {
		return
	}
	_ = coordinator.ReleaseOwner(ownerID)
}

func newResourceOwnerID(generationID string) (string, error) {
	return newResourceOwnerIDForRole("e", generationID)
}

func newResourceOwnerIDForRole(role, generationID string) (string, error) {
	if role != "e" && role != "c" {
		return "", errors.New("runtime resource process role is invalid")
	}
	if !generationPattern.MatchString(generationID) {
		return "", errors.New("runtime resource generation identity is invalid")
	}
	instanceID, err := newGenerationID()
	if err != nil {
		return "", err
	}
	return role + generationID + "-" + instanceID, nil
}

// initialEngineRecoveryMode prevents a first-run or recovery-required setup
// Host from running archive recovery writes before installation is ready.
// Legacy installations are reconciled to ready before this decision, so they
// keep normal restart recovery behavior.
func initialEngineRecoveryMode(state installation.State) string {
	if state == installation.StateReady {
		return "recover"
	}
	return "fresh"
}

type coldGenerationReconciler interface {
	ReconcileColdStart(activeGenerationID string) error
}

// reconcileColdGenerationState is intentionally called only after the startup
// Engine has passed its authenticated readiness check. In recover mode that
// readiness follows WithFencedRecovery and mutating LoadAll, so clearing the
// durable lease projection and detaching unreattached generations cannot
// retire an archive that is still being recovered. Fresh setup startup does
// not have authority to reconcile existing leases.
func reconcileColdGenerationState(registry coldGenerationReconciler, recoveryMode, activeGenerationID string, engineReady bool) error {
	if recoveryMode != "recover" {
		return nil
	}
	if !engineReady || registry == nil || !generationPattern.MatchString(activeGenerationID) {
		return errors.New("cold recovery has not completed")
	}
	return registry.ReconcileColdStart(activeGenerationID)
}

func resourceIPCPaths(ipcDir, hostBootID string) (socketPath, tokenPath string, err error) {
	if !filepath.IsAbs(ipcDir) || filepath.Clean(ipcDir) != ipcDir || !generationPattern.MatchString(hostBootID) {
		return "", "", errors.New("Runtime Host IPC identity is invalid")
	}
	// Keep the Unix socket basename well within platform limits even when the
	// operator's DATA_DIR path is long. The suffix remains a 64-bit prefix of
	// the fresh CSPRNG boot identity; credential filenames use the full ID.
	bootSuffix := hostBootID[:16]
	socketPath = filepath.Join(ipcDir, "r-"+bootSuffix+".sock")
	tokenPath = filepath.Join(ipcDir, "r-"+hostBootID+".token")
	if err := validateSocketPath(socketPath); err != nil || !filepath.IsAbs(tokenPath) || filepath.Clean(tokenPath) != tokenPath {
		return "", "", errors.New("Runtime Host IPC path is invalid")
	}
	return socketPath, tokenPath, nil
}

func generationIPCPaths(ipcDir, generationID, hostBootID string) (engineSocket, engineToken, controlSocket, controlToken string, err error) {
	if !filepath.IsAbs(ipcDir) || filepath.Clean(ipcDir) != ipcDir || !generationPattern.MatchString(generationID) || !generationPattern.MatchString(hostBootID) {
		return "", "", "", "", errors.New("generation IPC identity is invalid")
	}
	bootSuffix := "-" + hostBootID[:16]
	prefix := generationID[:12]
	engineSocket = filepath.Join(ipcDir, "e-"+prefix+bootSuffix+".sock")
	engineToken = filepath.Join(ipcDir, "e-"+prefix+bootSuffix+".token")
	controlSocket = filepath.Join(ipcDir, "c-"+prefix+bootSuffix+".sock")
	controlToken = filepath.Join(ipcDir, "c-"+prefix+bootSuffix+".token")
	if validateSocketPath(engineSocket) != nil || validateSocketPath(controlSocket) != nil ||
		!filepath.IsAbs(engineToken) || filepath.Clean(engineToken) != engineToken ||
		!filepath.IsAbs(controlToken) || filepath.Clean(controlToken) != controlToken {
		return "", "", "", "", errors.New("generation IPC path is invalid")
	}
	return engineSocket, engineToken, controlSocket, controlToken, nil
}

// releaseEngineResources is retained as a narrow helper for older package
// tests; production releases both Engine and Control process-owned leases.
func releaseEngineResources(coordinator *resources.Coordinator, spec supervisor.ProcessSpec) {
	if spec.Role != supervisor.RoleEngine {
		return
	}
	releaseProcessResources(coordinator, spec)
}

func childEnvValue(environment []string, key string) string {
	prefix := key + "="
	for _, item := range environment {
		if strings.HasPrefix(item, prefix) {
			return strings.TrimPrefix(item, prefix)
		}
	}
	return ""
}

func commonChildEnv(config Config, adapterDirectory string) []string {
	env := []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"HOME=" + config.DataDir,
		"TMPDIR=/tmp",
		"DATA_DIR=" + config.DataDir,
		"RUNTIME_INSTALLATION_MANAGED=1",
		"ADAPTER_DIR=" + adapterDirectory,
	}
	if config.FFmpegPath != "" {
		env = append(env, "FFMPEG_PATH="+config.FFmpegPath)
	}
	return env
}

// storageProviderChildEnv pins application processes to the immutable storage
// provider set recorded in their Runtime Host generation. Empty set identity
// is passed through as empty and causes OpenStore to fail closed.
func storageProviderChildEnv(dataDir, setID string) []string {
	return []string{
		"STORAGE_PROVIDER_CATALOG_ROOT=" + filepath.Join(dataDir, "runtime", "storage-providers"),
		"STORAGE_PROVIDER_SET_ID=" + setID,
	}
}

func configuredAdapterSourceDirs(value string) ([]string, error) {
	parts := filepath.SplitList(value)
	if len(parts) == 0 || len(parts) > 32 {
		return nil, errors.New("adapter source directory count is invalid")
	}
	dirs := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || !filepath.IsAbs(part) || filepath.Clean(part) != part || strings.ContainsRune(part, 0) {
			return nil, errors.New("adapter source directory is invalid")
		}
		dirs = append(dirs, part)
	}
	return dirs, nil
}

func validateOperatorAdapterDirs(value, bundledHLSBinary string) error {
	dirs, err := configuredAdapterSourceDirs(value)
	if err != nil {
		return errors.New("configured adapter source directories are invalid")
	}
	bundledDir := filepath.Dir(bundledHLSBinary)
	for _, dir := range dirs {
		// /adapters is an image-owned namespace, not an operator source. Also
		// reject the exact bundled executable's parent so an operator directory
		// override cannot introduce a same-name candidate beside it.
		if dir == "/adapters" || dir == bundledDir {
			return errors.New("operator adapter sources may not overlap the bundled source directory")
		}
	}
	return nil
}

func configuredAdapterSources(config Config, registrySources []adaptercatalog.Source) ([]adaptercatalog.Source, error) {
	sources := make([]adaptercatalog.Source, 0, 1+len(registrySources)+8)
	sources = append(sources, adaptercatalog.Source{
		Path: config.BundledHLSBinary, Attestation: plugintrust.NewBundled(), AllowedIDs: []string{"hls"},
	})
	sources = append(sources, registrySources...)
	if !config.AllowOperatorPlugins {
		return sources, nil
	}
	dirs, err := configuredAdapterSourceDirs(config.AdapterDirs)
	if err != nil || validateOperatorAdapterDirs(config.AdapterDirs, config.BundledHLSBinary) != nil {
		return nil, errors.New("configured adapter source directories are invalid")
	}
	for _, dir := range dirs {
		sources = append(sources, adaptercatalog.Source{Path: dir, Attestation: plugintrust.NewOperator()})
	}
	return sources, nil
}

func marshalAdapterTrust(snapshot adaptercatalog.Snapshot) (string, error) {
	if snapshot.ID == "" || len(snapshot.Entries) > 256 {
		return "", errors.New("adapter trust snapshot is invalid")
	}
	projection := make(map[string]plugintrust.Attestation, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		if !adapterproto.IsValidIdentifier(entry.AdapterID) {
			return "", errors.New("adapter trust identity is invalid")
		}
		attestation := entry.EffectiveAttestation()
		if attestation != plugintrust.Legacy() && attestation.Validate() != nil {
			return "", errors.New("adapter trust attestation is invalid")
		}
		if _, exists := projection[entry.AdapterID]; exists {
			return "", errors.New("adapter trust identity is duplicated")
		}
		projection[entry.AdapterID] = attestation
	}
	data, err := json.Marshal(projection)
	if err != nil || len(data) > maxControlTrustBytes {
		return "", errors.New("adapter trust projection exceeds its bound")
	}
	return string(data), nil
}

// reconcileStartupAdapterSet uses only a previously persisted active
// generation's immutable set as a rejection fallback. A fresh installation
// has no good set to preserve yet, so valid candidates must still be imported
// when a different source binary is rejected.
func reconcileStartupAdapterSet(ctx context.Context, catalog hostAdapterCatalog, registryState generation.Snapshot, additionalSources ...string) (adaptercatalog.Snapshot, generation.Generation, bool, error) {
	sources := make([]adaptercatalog.Source, 0, len(additionalSources))
	for _, path := range additionalSources {
		sources = append(sources, adaptercatalog.Source{Path: path, Attestation: plugintrust.NewOperator()})
	}
	return reconcileStartupAdapterSetClassified(ctx, catalog, registryState, sources)
}

func reconcileStartupAdapterSetClassified(ctx context.Context, catalog hostAdapterCatalog, registryState generation.Snapshot, additionalSources []adaptercatalog.Source) (adaptercatalog.Snapshot, generation.Generation, bool, error) {
	if catalog == nil {
		return adaptercatalog.Snapshot{}, generation.Generation{}, false, errors.New("Runtime Host adapter catalog is unavailable")
	}
	fallbackSetID := ""
	var activeGeneration generation.Generation
	activeExists := false
	if registryState.ActiveGenerationID != "" {
		activeGeneration, activeExists = registryState.Generations[registryState.ActiveGenerationID]
		if !activeExists || activeGeneration.State != generation.StateActive {
			return adaptercatalog.Snapshot{}, generation.Generation{}, false, errors.New("active adapter generation is inconsistent")
		}
		fallbackSetID = activeGeneration.AdapterSetID
	}
	var selected adaptercatalog.Snapshot
	var err error
	if classified, ok := catalog.(interface {
		ReconcileClassified(context.Context, string, []adaptercatalog.Source) (adaptercatalog.Snapshot, error)
	}); ok {
		selected, err = classified.ReconcileClassified(ctx, fallbackSetID, additionalSources)
	} else {
		if len(additionalSources) > 0 {
			return adaptercatalog.Snapshot{}, generation.Generation{}, false, errors.New("adapter catalog does not support Host-owned plugin sources")
		}
		selected, err = catalog.Reconcile(ctx, fallbackSetID)
	}
	if err != nil {
		return adaptercatalog.Snapshot{}, generation.Generation{}, false, errors.New("Runtime Host adapter discovery failed")
	}
	return selected, activeGeneration, activeExists, nil
}

func startPeriodicAdapterReconciliation(ctx context.Context, store *installation.Store, controller *updateController, interval time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if interval <= 0 {
			return
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if store == nil || store.Snapshot().State != installation.StateReady || controller == nil {
					continue
				}
				callCtx, cancel := context.WithTimeout(ctx, controller.operationTimeout)
				_, _ = controller.reconcileAdaptersIfIdle(callCtx)
				cancel()
			}
		}
	}()
	return done
}

func selectGeneration(snapshot generation.Snapshot, build buildinfo.Info) (string, bool, error) {
	if id := snapshot.ActiveGenerationID; id != "" {
		current, ok := snapshot.Generations[id]
		if ok && current.Version == build.Version && current.Commit == build.Commit && current.State == generation.StateActive {
			return id, false, nil
		}
	}
	id, err := newGenerationID()
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}

func runtimeGeneration(id string, build buildinfo.Info) generation.Generation {
	version := build.Version
	commit := build.Commit
	if strings.TrimSpace(version) == "" {
		version = "dev"
	}
	if strings.TrimSpace(commit) == "" {
		commit = "unknown"
	}
	return generation.Generation{
		ID: id, Version: version, Commit: commit, InstalledAt: time.Now().UTC(), State: generation.StateStaging,
		ControlProtocol: runtimeipc.ProtocolVersion, EngineProtocol: runtimeipc.ProtocolVersion,
		ArchiveReadCompatibility: generation.CompatibilityRange{Minimum: generation.CurrentArchiveFormatVersion, Maximum: generation.CurrentArchiveFormatVersion}, ArchiveWriteFormat: generation.CurrentArchiveFormatVersion,
	}
}

type startupStorageCatalog interface {
	LoadSet(string) (storagecatalog.Set, error)
	LoadStorageInstance(string) (storagecatalog.StorageInstance, error)
}

// resolveStartupStoragePin selects the current immutable set for an active
// named instance. The active generation keeps its original set pin; callers
// stage a new generation when the instance's desired set changed.
func resolveStartupStoragePin(catalog startupStorageCatalog, state generation.Snapshot, localSetID string) (string, string, error) {
	if catalog == nil || localSetID == "" {
		return "", "", errors.New("Runtime Host storage provider set is unavailable")
	}
	storageSetID := localSetID
	storageInstanceID := storagecatalog.LegacyStorageInstanceID("local")
	active, exists := state.Generations[state.ActiveGenerationID]
	if !exists || state.ActiveGenerationID == "" {
		return storageSetID, storageInstanceID, nil
	}
	if active.StorageProviderSetID == "" {
		if active.StorageInstanceID != "" {
			return "", "", errors.New("active Runtime Host storage instance is unavailable")
		}
		return storageSetID, storageInstanceID, nil
	}
	activeSet, err := catalog.LoadSet(active.StorageProviderSetID)
	if err != nil {
		return "", "", errors.New("active Runtime Host storage provider set is unavailable")
	}
	if active.StorageInstanceID != "" {
		instance, err := catalog.LoadStorageInstance(active.StorageInstanceID)
		if err != nil || instance.ID != active.StorageInstanceID || instance.ProviderID != activeSet.Artifact.ID {
			return "", "", errors.New("active Runtime Host storage instance is unavailable")
		}
		desiredSet, err := catalog.LoadSet(instance.DesiredSetID)
		if err != nil || desiredSet.Artifact.ID != instance.ProviderID {
			return "", "", errors.New("active Runtime Host storage instance set is unavailable")
		}
		return desiredSet.ID, instance.ID, nil
	}

	// Legacy generations have no named instance pin. A selected external
	// provider survives restart; a bundled local artifact change rolls forward
	// the legacy local instance without changing existing recording pins.
	if activeSet.Artifact.ID != "local" {
		storageSetID = active.StorageProviderSetID
	}
	storageInstanceID = storagecatalog.LegacyStorageInstanceID(activeSet.Artifact.ID)
	return storageSetID, storageInstanceID, nil
}

// prepareStartupGeneration binds the selected application release to both
// immutable source-adapter and physical-storage sets. A set change over an
// existing active application allocates a new generation identity and copies
// the exact application compatibility tuple; it never edits the old one.
func prepareStartupGeneration(selected runtimeRelease, registryState generation.Snapshot, adapterSetID, storageProviderSetID, storageInstanceID string) (runtimeRelease, generation.Generation, bool, error) {
	if adapterSetID == "" || storageProviderSetID == "" || storageInstanceID == "" {
		return runtimeRelease{}, generation.Generation{}, false, errors.New("adapter or storage provider set identity is unavailable")
	}
	if registryState.ActiveGenerationID != "" {
		active, ok := registryState.Generations[registryState.ActiveGenerationID]
		if !ok || active.State != generation.StateActive {
			return runtimeRelease{}, generation.Generation{}, false, errors.New("active application generation is inconsistent")
		}
		if active.AdapterSetID != adapterSetID || active.StorageProviderSetID != storageProviderSetID || active.StorageInstanceID != storageInstanceID {
			id, err := newGenerationID()
			if err != nil {
				return runtimeRelease{}, generation.Generation{}, false, err
			}
			candidate := active
			candidate.ID = id
			candidate.AdapterSetID = adapterSetID
			candidate.StorageProviderSetID = storageProviderSetID
			candidate.StorageInstanceID = storageInstanceID
			candidate.InstalledAt = time.Now().UTC()
			candidate.State = generation.StateStaging
			candidate.EngineDormant = false
			selected.generationID = id
			selected.needsStage = true
			return selected, candidate, true, nil
		}
		return selected, active, false, nil
	}
	if !selected.needsStage || !generationPattern.MatchString(selected.generationID) {
		return runtimeRelease{}, generation.Generation{}, false, errors.New("initial application generation is inconsistent")
	}
	candidate := runtimeGeneration(selected.generationID, selected.appBuild)
	candidate.AdapterSetID = adapterSetID
	candidate.StorageProviderSetID = storageProviderSetID
	candidate.StorageInstanceID = storageInstanceID
	return selected, candidate, true, nil
}

func newGenerationID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", errors.New("runtime generation identity could not be created")
	}
	return hex.EncodeToString(id[:]), nil
}

func randomSecret() ([]byte, error) {
	secret := make([]byte, maximumTokenFileSize)
	if _, err := rand.Read(secret); err != nil {
		return nil, errors.New("runtime IPC credential could not be created")
	}
	return secret, nil
}

func writeAtomicPrivate(path string, contents []byte) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(contents) == 0 || len(contents) > 1<<20 {
		return errors.New("private atomic file parameters are invalid")
	}
	dir := filepath.Dir(path)
	if err := requirePrivateDirectory(dir); err != nil {
		return err
	}
	if current, err := os.Lstat(path); err == nil {
		if !current.Mode().IsRegular() || current.Mode()&os.ModeSymlink != 0 {
			return errors.New("private file target is not a regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".host-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(contents); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func writeEngineCatalog(path string, catalog controlplane.EngineCatalog) error {
	if err := catalog.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(catalog)
	if err != nil {
		return errors.New("engine catalog could not be encoded")
	}
	return writeAtomicPrivate(path, data)
}

func makePrivateRuntimeDirs(dataDir string) error {
	if err := mkdirNoSymlinkParents(dataDir, 0750); err != nil {
		return fmt.Errorf("prepare DATA_DIR: %w", err)
	}
	for _, path := range []string{filepath.Join(dataDir, "runtime"), filepath.Join(dataDir, "runtime", "ipc"), filepath.Join(dataDir, "runtime", "state")} {
		if err := ensurePrivateDirectory(path); err != nil {
			return fmt.Errorf("prepare private Runtime Host directory: %w", err)
		}
	}
	return nil
}

func mkdirNoSymlinkParents(path string, finalMode os.FileMode) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("directory path must be absolute and clean")
	}
	current := string(filepath.Separator)
	for _, part := range strings.Split(strings.TrimPrefix(path, current), current) {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			mode := os.FileMode(0755)
			if current == path {
				mode = finalMode
			}
			if err := os.Mkdir(current, mode); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			info, err = os.Lstat(current)
		}
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("path component is unavailable or a symlink")
		}
	}
	return nil
}

func ensurePrivateDirectory(path string) error {
	if err := mkdirNoSymlinkParents(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("private directory is unavailable")
	}
	if info.Mode().Perm() != 0700 {
		if err := os.Chmod(path, 0700); err != nil {
			return errors.New("private directory permissions could not be secured")
		}
	}
	return requirePrivateDirectory(path)
}

func requirePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 {
		return errors.New("private directory must be a non-symlink 0700 directory")
	}
	return nil
}

func validateBundle(bundle string) error {
	if err := rejectSymlinkPath(bundle); err != nil {
		return errors.New("bundled release path contains an unsafe symlink")
	}
	if err := requireDirectory(bundle); err != nil {
		return errors.New("bundled release directory is unavailable or unsafe")
	}
	for _, name := range []string{"control-plane", "recorder-engine"} {
		path := filepath.Join(bundle, name)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0111 == 0 || info.Mode().Perm()&0222 != 0 {
			return fmt.Errorf("bundled %s executable is unavailable or unsafe", name)
		}
	}
	return nil
}

func rejectSymlinkPath(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("bundle path must be absolute and clean")
	}
	current := string(filepath.Separator)
	for _, part := range strings.Split(strings.TrimPrefix(path, current), current) {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("bundle path contains a symlink or unavailable component")
		}
	}
	return nil
}

func requireDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("directory is unavailable or unsafe")
	}
	return nil
}

func validateListenAddress(address string) error {
	if strings.TrimSpace(address) == "" || strings.ContainsRune(address, 0) {
		return errors.New("ADDR is invalid")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return errors.New("ADDR must be a TCP host:port address")
	}
	parsedPort, portErr := strconv.Atoi(port)
	if portErr != nil || parsedPort < 1 || parsedPort > 65535 {
		return errors.New("ADDR port must be between 1 and 65535")
	}
	if host != "" && net.ParseIP(host) == nil {
		return errors.New("ADDR host must be an IP address")
	}
	return nil
}

func listenIsLoopback(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func boolEnv(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func privateControlTarget() (string, *url.URL, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, errors.New("private Control listener address could not be allocated")
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", nil, errors.New("private Control listener reservation could not be released")
	}
	target, err := url.Parse("http://" + address)
	if err != nil {
		return "", nil, errors.New("private Control listener address is invalid")
	}
	return address, target, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func validateSocketPath(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 100 {
		return errors.New("Runtime Host IPC socket path exceeds the supported local socket limit")
	}
	return nil
}

type endpoint struct {
	socket string
	token  string
}

type processReadiness struct {
	wait      time.Duration
	mu        chan struct{}
	endpoints map[string]endpoint
	engines   map[string]recorderengine.ReadyResult
	controls  map[string]controlplane.LifecycleSnapshot
}

func newProcessReadiness(wait time.Duration) *processReadiness {
	r := &processReadiness{wait: wait, mu: make(chan struct{}, 1), endpoints: make(map[string]endpoint), engines: make(map[string]recorderengine.ReadyResult), controls: make(map[string]controlplane.LifecycleSnapshot)}
	r.mu <- struct{}{}
	return r
}

func (r *processReadiness) register(generationID string, role supervisor.Role, socket, token string) {
	<-r.mu
	r.endpoints[generationID+"/"+string(role)] = endpoint{socket: socket, token: token}
	r.mu <- struct{}{}
}

func (r *processReadiness) engineIdentity(generationID string) (recorderengine.ReadyResult, error) {
	<-r.mu
	defer func() { r.mu <- struct{}{} }()
	result, ok := r.engines[generationID]
	if !ok {
		return recorderengine.ReadyResult{}, errors.New("Recorder Engine readiness identity was not recorded")
	}
	return result, nil
}

func (r *processReadiness) controlIdentity(generationID string) (controlplane.LifecycleSnapshot, error) {
	<-r.mu
	defer func() { r.mu <- struct{}{} }()
	result, ok := r.controls[generationID]
	if !ok {
		return controlplane.LifecycleSnapshot{}, errors.New("Control Plane readiness identity was not recorded")
	}
	return result, nil
}

func (r *processReadiness) WaitReady(ctx context.Context, spec supervisor.ProcessSpec, child supervisor.Child) error {
	if ctx == nil || child == nil || r.wait <= 0 {
		return supervisor.ErrInvalidConfig
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, r.wait)
	defer cancel()
	<-r.mu
	e, ok := r.endpoints[spec.GenerationID+"/"+string(spec.Role)]
	r.mu <- struct{}{}
	if !ok {
		return supervisor.ErrCandidateNotReady
	}
	token, err := controlplane.LoadPrivateIPCSecret(e.token)
	if err != nil {
		return errors.New("runtime process credential is unavailable")
	}
	client, err := runtimeipc.NewClient(e.socket, spec.GenerationID, token, 3*time.Second)
	if err != nil {
		return errors.New("runtime process readiness client is invalid")
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-child.Done():
			return supervisor.ErrCandidateNotReady
		default:
		}
		if spec.Role == supervisor.RoleEngine {
			var ready recorderengine.ReadyResult
			err = client.Call(deadlineCtx, recorderengine.OperationReady, nil, &ready)
			if err == nil && ready.Ready && ready.GenerationID == spec.GenerationID && ready.InstanceID != "" && ready.ProtocolVersion == runtimeipc.ProtocolVersion {
				strict, strictErr := runtimeipc.NewClientForInstance(e.socket, spec.GenerationID, ready.InstanceID, token, 3*time.Second)
				var verified recorderengine.ReadyResult
				if strictErr == nil {
					strictErr = strict.Call(deadlineCtx, recorderengine.OperationReady, nil, &verified)
				}
				if strictErr != nil || !verified.Ready || verified.GenerationID != spec.GenerationID || verified.InstanceID != ready.InstanceID || verified.ProtocolVersion != runtimeipc.ProtocolVersion {
					select {
					case <-deadlineCtx.Done():
						return errors.New("runtime process readiness timed out")
					case <-child.Done():
						return supervisor.ErrCandidateNotReady
					case <-ticker.C:
						continue
					}
				}
				<-r.mu
				r.engines[spec.GenerationID] = verified
				r.mu <- struct{}{}
				return nil
			}
		} else {
			var ready controlplane.LifecycleSnapshot
			err = client.Call(deadlineCtx, controlplane.OperationControlReady, nil, &ready)
			if err == nil && ready.Ready && !ready.Active && ready.State == controlplane.LifecyclePassive && ready.GenerationID == spec.GenerationID && ready.InstanceID != "" {
				strict, strictErr := runtimeipc.NewClientForInstance(e.socket, spec.GenerationID, ready.InstanceID, token, 3*time.Second)
				var verified controlplane.LifecycleSnapshot
				if strictErr == nil {
					strictErr = strict.Call(deadlineCtx, controlplane.OperationControlReady, nil, &verified)
				}
				if strictErr == nil && verified.Ready && !verified.Active && verified.State == controlplane.LifecyclePassive && verified.GenerationID == spec.GenerationID && verified.InstanceID == ready.InstanceID {
					<-r.mu
					r.controls[spec.GenerationID] = verified
					r.mu <- struct{}{}
					return nil
				}
			}
		}
		select {
		case <-deadlineCtx.Done():
			return errors.New("runtime process readiness timed out")
		case <-child.Done():
			return supervisor.ErrCandidateNotReady
		case <-ticker.C:
		}
	}
}

type controlLifecycle struct {
	mu       chan struct{}
	clients  map[string]*runtimeipc.Client
	activeID string
}

func newControlLifecycle() *controlLifecycle {
	c := &controlLifecycle{mu: make(chan struct{}, 1), clients: make(map[string]*runtimeipc.Client)}
	c.mu <- struct{}{}
	return c
}

func (c *controlLifecycle) register(generationID, socket, tokenPath, instanceID string) error {
	token, err := controlplane.LoadPrivateIPCSecret(tokenPath)
	if err != nil {
		return errors.New("Control lifecycle credential is unavailable")
	}
	client, err := runtimeipc.NewClientForInstance(socket, generationID, instanceID, token, controlPrepareWait)
	if err != nil {
		return errors.New("Control lifecycle instance identity is invalid")
	}
	<-c.mu
	c.clients[generationID] = client
	c.mu <- struct{}{}
	return nil
}

func (c *controlLifecycle) call(ctx context.Context, generationID, operation string) (controlplane.LifecycleSnapshot, error) {
	return c.callPayload(ctx, generationID, operation, nil)
}

func (c *controlLifecycle) callPayload(ctx context.Context, generationID, operation string, payload any) (controlplane.LifecycleSnapshot, error) {
	<-c.mu
	client := c.clients[generationID]
	c.mu <- struct{}{}
	if client == nil {
		return controlplane.LifecycleSnapshot{}, errors.New("Control lifecycle IPC endpoint is unavailable")
	}
	var snapshot controlplane.LifecycleSnapshot
	if err := client.Call(ctx, operation, payload, &snapshot); err != nil {
		return controlplane.LifecycleSnapshot{}, err
	}
	if snapshot.GenerationID != generationID {
		return controlplane.LifecycleSnapshot{}, errors.New("Control lifecycle response generation mismatch")
	}
	return snapshot, nil
}

// AppendAudit forwards only to the Host-selected active Control. The Control
// lifecycle checks ACTIVE again while serialized with handoff, so a stale Host
// selection cannot append through a passive generation.
func (c *controlLifecycle) AppendAudit(ctx context.Context, request controlplane.AuditAppendRequest) error {
	if c == nil || ctx == nil || request.Validate() != nil {
		return errors.New("active Control audit append is unavailable")
	}
	generationID := c.activeGenerationID()
	if generationID == "" {
		return errors.New("active Control audit append is unavailable")
	}
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	snapshot, err := c.callPayload(callCtx, generationID, controlplane.OperationControlAppendAudit, request)
	if err != nil || !snapshot.Active || snapshot.State != controlplane.LifecycleActive {
		return errors.New("active Control audit append was not confirmed")
	}
	return nil
}

func (c *controlLifecycle) PrepareHandoff(ctx context.Context, oldID, _ string) error {
	if oldID == "" {
		return nil
	}
	snapshot, err := c.call(ctx, oldID, controlplane.OperationControlPrepareHandoff)
	if err != nil || snapshot.State != controlplane.LifecyclePreparing {
		return errors.New("active Control could not prepare for handoff")
	}
	return nil
}

func (c *controlLifecycle) PrepareActivation(ctx context.Context, generationID string) error {
	snapshot, err := c.call(ctx, generationID, controlplane.OperationControlPrepareActivation)
	if err != nil || !snapshot.Ready || !snapshot.Prepared || snapshot.Active || snapshot.State != controlplane.LifecyclePassive {
		return errors.New("candidate Control application preparation was not confirmed")
	}
	return nil
}

func (c *controlLifecycle) Activate(ctx context.Context, generationID string) error {
	snapshot, err := c.call(ctx, generationID, controlplane.OperationControlActivate)
	if err != nil || !snapshot.Ready || !snapshot.Prepared || snapshot.State != controlplane.LifecycleActive || !snapshot.Active {
		return errors.New("candidate Control activation was not confirmed")
	}
	<-c.mu
	c.activeID = generationID
	c.mu <- struct{}{}
	return nil
}

func (c *controlLifecycle) ValidateInstallation(ctx context.Context) error {
	snapshot, err := c.call(ctx, c.activeGenerationID(), controlplane.OperationControlValidateInstall)
	if err != nil || !snapshot.Active || snapshot.State != controlplane.LifecycleActive {
		return errors.New("active Control installation validation was not confirmed")
	}
	return nil
}

func (c *controlLifecycle) activeGenerationID() string {
	<-c.mu
	id := c.activeID
	c.mu <- struct{}{}
	return id
}

func (c *controlLifecycle) InstallationReady(ctx context.Context) error {
	snapshot, err := c.call(ctx, c.activeGenerationID(), controlplane.OperationControlInstallReady)
	if err != nil || !snapshot.Active || snapshot.State != controlplane.LifecycleActive {
		return errors.New("active Control installation activation was not confirmed")
	}
	return nil
}

func (c *controlLifecycle) Rollback(ctx context.Context, oldID, newID string) error {
	var errs []error
	if newID != "" {
		if _, err := c.call(ctx, newID, controlplane.OperationControlDeactivate); err != nil {
			errs = append(errs, errors.New("candidate Control could not be deactivated"))
		}
	}
	if oldID != "" {
		if snapshot, err := c.call(ctx, oldID, controlplane.OperationControlResume); err != nil || snapshot.State != controlplane.LifecycleActive {
			errs = append(errs, errors.New("previous Control could not resume"))
		}
	}
	if len(errs) == 0 && oldID != "" {
		<-c.mu
		c.activeID = oldID
		c.mu <- struct{}{}
	}
	return errors.Join(errs...)
}

func (c *controlLifecycle) DetachEngine(ctx context.Context, controlGenerationID, engineGenerationID string) error {
	if controlGenerationID == "" || engineGenerationID == "" || !generationPattern.MatchString(controlGenerationID) || !generationPattern.MatchString(engineGenerationID) {
		return errors.New("Control or Engine generation identity is invalid")
	}
	<-c.mu
	client := c.clients[controlGenerationID]
	c.mu <- struct{}{}
	if client == nil {
		return errors.New("active Control lifecycle IPC endpoint is unavailable")
	}
	payload := struct {
		GenerationID string `json:"generation_id"`
	}{GenerationID: engineGenerationID}
	var snapshot controlplane.LifecycleSnapshot
	if err := client.Call(ctx, controlplane.OperationControlDetachEngine, payload, &snapshot); err != nil {
		return errors.New("active Control could not detach the drained Engine")
	}
	if snapshot.GenerationID != controlGenerationID || snapshot.State != controlplane.LifecycleActive || !snapshot.Active {
		return errors.New("Control Engine detach was not confirmed by the active generation")
	}
	return nil
}

type engineDrain struct {
	mu      chan struct{}
	clients map[string]*recorderengine.ManagerClient
}

func newEngineDrain() *engineDrain {
	d := &engineDrain{mu: make(chan struct{}, 1), clients: make(map[string]*recorderengine.ManagerClient)}
	d.mu <- struct{}{}
	return d
}

func (d *engineDrain) register(generationID, socket, tokenPath, instanceID string) error {
	token, err := controlplane.LoadPrivateIPCSecret(tokenPath)
	if err != nil {
		return err
	}
	client, err := runtimeipc.NewClientForInstance(socket, generationID, instanceID, token, 3*time.Second)
	if err != nil {
		return err
	}
	manager, err := recorderengine.NewManagerClient(client)
	if err != nil {
		return err
	}
	<-d.mu
	d.clients[generationID] = manager
	d.mu <- struct{}{}
	return nil
}

func (d *engineDrain) manager(generationID string) (*recorderengine.ManagerClient, error) {
	<-d.mu
	manager := d.clients[generationID]
	d.mu <- struct{}{}
	if manager == nil {
		return nil, errors.New("Recorder Engine drain endpoint is unavailable")
	}
	return manager, nil
}

func (d *engineDrain) BeginDrain(ctx context.Context, generationID string) error {
	manager, err := d.manager(generationID)
	if err != nil {
		return err
	}
	return manager.BeginDrain(ctx, generationID)
}

func (d *engineDrain) ActiveRecordings(ctx context.Context, generationID string) (int, error) {
	manager, err := d.manager(generationID)
	if err != nil {
		return 0, err
	}
	return manager.ActiveRecordings(ctx, generationID)
}

var _ supervisor.Readiness = (*processReadiness)(nil)
var _ supervisor.ControlLifecycle = (*controlLifecycle)(nil)
var _ supervisor.EngineDrain = (*engineDrain)(nil)
