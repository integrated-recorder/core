package bootstrap

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/plugintrust"
	"github.com/integrated-recorder/core/internal/runtimehost/adaptercatalog"
	"github.com/integrated-recorder/core/internal/runtimehost/generation"
	"github.com/integrated-recorder/core/internal/runtimehost/httpapi"
	"github.com/integrated-recorder/core/internal/runtimehost/installation"
	"github.com/integrated-recorder/core/internal/runtimehost/pluginregistry"
	"github.com/integrated-recorder/core/internal/runtimehost/storagecatalog"
	"github.com/integrated-recorder/core/internal/storageproto"
)

type classifiedAdapterCatalog interface {
	ReconcileClassified(context.Context, string, []adaptercatalog.Source) (adaptercatalog.Snapshot, error)
}

func (c *updateController) reconcileCatalog(ctx context.Context, fallbackSetID string) (adaptercatalog.Snapshot, error) {
	sources := []adaptercatalog.Source{{
		Path: c.config.BundledHLSBinary, Attestation: plugintrust.NewBundled(), AllowedIDs: []string{"hls"},
	}}
	if c.pluginRegistry != nil {
		registrySources, err := c.pluginRegistry.DesiredSources()
		if err != nil {
			return adaptercatalog.Snapshot{}, err
		}
		sources = append(sources, registrySources...)
	}
	if c.config.AllowOperatorPlugins {
		if err := validateOperatorAdapterDirs(c.config.AdapterDirs, c.config.BundledHLSBinary); err != nil {
			return adaptercatalog.Snapshot{}, err
		}
		dirs, err := configuredAdapterSourceDirs(c.config.AdapterDirs)
		if err != nil {
			return adaptercatalog.Snapshot{}, err
		}
		for _, dir := range dirs {
			sources = append(sources, adaptercatalog.Source{Path: dir, Attestation: plugintrust.NewOperator()})
		}
	}
	if classified, ok := c.adapterCatalog.(classifiedAdapterCatalog); ok {
		return classified.ReconcileClassified(ctx, fallbackSetID, sources)
	}
	// Small test doubles and legacy embedders may implement only Reconcile. Do
	// not silently drop any real classified source when taking that fallback.
	for _, source := range sources {
		if _, err := os.Lstat(source.Path); err == nil || !errors.Is(err, os.ErrNotExist) {
			return adaptercatalog.Snapshot{}, errors.New("adapter catalog does not support Host-owned plugin sources")
		}
	}
	return c.adapterCatalog.Reconcile(ctx, fallbackSetID)
}

func (c *updateController) PluginStatus(ctx context.Context) (httpapi.PluginStatus, error) {
	if ctx == nil || ctx.Err() != nil {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("internal_error")
	}
	return c.pluginStatus(), nil
}

func (c *updateController) RefreshPlugins(ctx context.Context) (httpapi.PluginStatus, error) {
	operationCtx, cancel, err := c.beginPluginOperation(ctx)
	if err != nil {
		return httpapi.PluginStatus{}, err
	}
	defer cancel()
	defer c.releaseOperation()
	if c.pluginRegistry == nil {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_registry_unavailable")
	}
	if err := c.pluginRegistry.Refresh(operationCtx); err != nil {
		return c.pluginStatus(), mapPluginRegistryError(err)
	}
	return c.pluginStatus(), nil
}

func (c *updateController) InstallPlugin(ctx context.Context, id string) (httpapi.PluginStatus, error) {
	if id == "local" {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_operation_conflict")
	}
	if c.pluginType(id) == pluginregistry.TypeStorage {
		return c.installStoragePlugin(ctx, id, false)
	}
	return c.changePlugin(ctx, id, false)
}

func (c *updateController) UpdatePlugin(ctx context.Context, id string) (httpapi.PluginStatus, error) {
	if id == "local" {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_operation_conflict")
	}
	if c.pluginType(id) == pluginregistry.TypeStorage {
		return c.installStoragePlugin(ctx, id, true)
	}
	return c.changePlugin(ctx, id, true)
}

func (c *updateController) UninstallPlugin(ctx context.Context, id string) (httpapi.PluginStatus, error) {
	if id == "local" {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_operation_conflict")
	}
	if c.isStoragePluginInstalled(id) {
		return c.uninstallStoragePlugin(ctx, id)
	}
	operationCtx, cancel, err := c.beginPluginOperation(ctx)
	if err != nil {
		return httpapi.PluginStatus{}, err
	}
	defer cancel()
	defer c.releaseOperation()
	if c.pluginRegistry == nil {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_registry_unavailable")
	}
	if c.registry.Snapshot().StagedGenerationID != "" {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_operation_conflict")
	}
	plan, err := c.pluginRegistry.PrepareUninstall(id)
	if err != nil {
		return httpapi.PluginStatus{}, mapPluginRegistryError(err)
	}
	defer func() {
		plan.Close()
		_ = c.pluginRegistry.CollectGarbage()
	}()
	if err := plan.Commit(); err != nil {
		return httpapi.PluginStatus{}, mapPluginRegistryError(err)
	}
	result, err := c.reconcileAdaptersLocked(operationCtx)
	if err != nil || result.State != "activated" && result.State != "unchanged" && result.State != "rejected" {
		_ = plan.Rollback()
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_install_failed")
	}
	if err := c.verifyActivePluginAbsent(id); err != nil {
		_ = plan.Rollback()
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_install_failed")
	}
	return c.pluginStatus(), nil
}

func (c *updateController) installStoragePlugin(ctx context.Context, id string, update bool) (httpapi.PluginStatus, error) {
	operationCtx, cancel, err := c.beginPluginOperation(ctx)
	if err != nil {
		return httpapi.PluginStatus{}, err
	}
	defer cancel()
	defer c.releaseOperation()
	return c.installStoragePluginLocked(operationCtx, id, update)
}

// installStoragePluginLocked runs the existing manual storage-provider update
// lifecycle while the caller owns the common Host operation gate. It imports
// only a verified immutable artifact and uses the existing config carry-forward
// rule; it never changes the active physical backend by itself.
func (c *updateController) installStoragePluginLocked(operationCtx context.Context, id string, update bool) (httpapi.PluginStatus, error) {
	if c.pluginRegistry == nil || c.storageCatalog == nil {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_registry_unavailable")
	}
	if c.registry.Snapshot().StagedGenerationID != "" {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_operation_conflict")
	}
	installed, installedErr := c.installedStorageArtifact(id)
	if installedErr != nil && !errors.Is(installedErr, storagecatalog.ErrArtifactMissing) {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("storage_provider_unavailable")
	}
	if update {
		if installedErr != nil || c.pluginRegistry == nil || !c.pluginRegistry.UpdateAvailable(id, installed.Version) {
			return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_operation_conflict")
		}
	} else if installedErr == nil {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_operation_conflict")
	}
	var previousConfig *storagecatalog.SetConfig
	if update {
		previous, previousErr := c.storageCatalog.DesiredSet(id)
		if previousErr == nil {
			config := previous.Config
			previousConfig = &config
		} else if !errors.Is(previousErr, storagecatalog.ErrSetMissing) {
			return httpapi.PluginStatus{}, httpapi.NewControllerError("storage_provider_unavailable")
		}
	}
	plan, err := c.pluginRegistry.PrepareStorageArtifact(operationCtx, id)
	if err != nil {
		return httpapi.PluginStatus{}, mapPluginRegistryError(err)
	}
	defer plan.Close()
	selection := plan.Selection()
	if selection.Attestation == nil || selection.Attestation.Validate() != nil {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_install_failed")
	}
	artifact, err := c.storageCatalog.Import(operationCtx, plan.BinaryPath(), storagecatalog.Expected{
		ID: selection.ID, Version: selection.Version, ProtocolVersion: storageproto.Version,
		SHA256: selection.Digest, Size: selection.Size,
	})
	if err != nil {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_install_failed")
	}
	var migratedSet *storagecatalog.Set
	if update {
		if previousConfig != nil {
			// Carry forward the provider configuration to the new immutable
			// artifact when its schema still accepts it. Existing generations keep
			// their old set; this only changes the configured candidate. An
			// incompatible schema leaves the old desired set selected so the
			// current artifact is reported as needing configuration.
			if _, _, describeErr := c.storageCatalog.DescribeArtifact(artifact.Digest); describeErr != nil {
				return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_install_failed")
			}
		}
	}
	if err := c.storageCatalog.InstallWithAttestation(artifact, *selection.Attestation); err != nil {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_install_failed")
	}
	if update && previousConfig != nil {
		next, setErr := c.storageCatalog.CreateInstalledSet(id, *previousConfig)
		if setErr == nil {
			migratedSet = &next
		} else if !errors.Is(setErr, storagecatalog.ErrInvalidConfig) {
			return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_install_failed")
		}
	}
	if migratedSet != nil {
		if err := c.storageCatalog.SelectDesiredSet(id, migratedSet.ID); err != nil {
			return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_install_failed")
		}
	}
	return c.pluginStatus(), nil
}

func (c *updateController) uninstallStoragePlugin(ctx context.Context, id string) (httpapi.PluginStatus, error) {
	operationCtx, cancel, err := c.beginPluginOperation(ctx)
	if err != nil {
		return httpapi.PluginStatus{}, err
	}
	defer cancel()
	defer c.releaseOperation()
	if c.storageCatalog == nil || c.registry == nil {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("storage_provider_not_installed")
	}
	snapshot := c.registry.Snapshot()
	if snapshot.StagedGenerationID != "" {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_operation_conflict")
	}
	for _, item := range snapshot.Generations {
		if (item.State == generation.StateActive || item.State == generation.StateDraining || item.State == generation.StateStaging || item.State == generation.StateVerified || item.State == generation.StateReady) && item.StorageProviderSetID != "" {
			set, loadErr := c.storageCatalog.LoadSet(item.StorageProviderSetID)
			if loadErr == nil && set.Artifact.ID == id && item.State == generation.StateActive {
				return httpapi.PluginStatus{}, httpapi.NewControllerError("storage_backend_in_use")
			}
		}
	}
	if err := operationCtx.Err(); err != nil {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_operation_conflict")
	}
	if err := c.storageCatalog.Uninstall(id); err != nil {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("storage_provider_not_installed")
	}
	return c.pluginStatus(), nil
}

func (c *updateController) pluginType(id string) string {
	if c == nil || c.pluginRegistry == nil {
		return ""
	}
	for _, item := range c.pluginRegistry.View().Plugins {
		if item.ID == id {
			return item.Type
		}
	}
	return ""
}

func (c *updateController) isStoragePluginInstalled(id string) bool {
	if c == nil || c.storageCatalog == nil || id == "" {
		return false
	}
	items, err := c.storageCatalog.Installed()
	if err != nil {
		return false
	}
	for _, item := range items {
		if item.ID == id {
			return true
		}
	}
	return false
}

func (c *updateController) changePlugin(ctx context.Context, id string, update bool) (httpapi.PluginStatus, error) {
	operationCtx, cancel, err := c.beginPluginOperation(ctx)
	if err != nil {
		return httpapi.PluginStatus{}, err
	}
	defer cancel()
	defer c.releaseOperation()
	return c.changePluginLocked(operationCtx, id, update)
}

// changePluginLocked executes a source-plugin transaction while the caller
// owns the shared application/plugin operation gate.
func (c *updateController) changePluginLocked(operationCtx context.Context, id string, update bool) (httpapi.PluginStatus, error) {
	if c.pluginRegistry == nil {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_registry_unavailable")
	}
	if c.registry.Snapshot().StagedGenerationID != "" {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_operation_conflict")
	}
	plan, err := c.pluginRegistry.PrepareInstall(operationCtx, id, update)
	if err != nil {
		return httpapi.PluginStatus{}, mapPluginRegistryError(err)
	}
	defer func() {
		plan.Close()
		_ = c.pluginRegistry.CollectGarbage()
	}()
	selection := plan.Selection()
	if err := plan.Commit(); err != nil {
		return httpapi.PluginStatus{}, mapPluginRegistryError(err)
	}
	result, err := c.reconcileAdaptersLocked(operationCtx)
	if err != nil || result.State != "activated" && result.State != "unchanged" && result.State != "rejected" {
		_ = plan.Rollback()
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_install_failed")
	}
	if err := c.verifyActivePlugin(selection); err != nil {
		_ = plan.Rollback()
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_identity_mismatch")
	}
	return c.pluginStatus(), nil
}

func (c *updateController) beginPluginOperation(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, httpapi.NewControllerError("internal_error")
	}
	operationCtx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	if c.installation == nil || c.installation.Snapshot().State != installation.StateReady {
		cancel()
		return nil, nil, httpapi.NewControllerError("installation_incomplete")
	}
	if err := c.lockOperation(operationCtx, true); err != nil {
		cancel()
		return nil, nil, httpapi.NewControllerError("plugin_operation_conflict")
	}
	if operationCtx.Err() != nil {
		c.releaseOperation()
		cancel()
		return nil, nil, httpapi.NewControllerError("plugin_operation_conflict")
	}
	return operationCtx, cancel, nil
}

func (c *updateController) pluginStatus() httpapi.PluginStatus {
	view := pluginregistry.View{Plugins: []pluginregistry.PluginView{}}
	status := httpapi.PluginStatus{State: "not_configured", Plugins: []httpapi.PluginStatusItem{}}
	if c.pluginRegistry != nil {
		view = c.pluginRegistry.View()
		status.State = "ready"
		if !view.Configured {
			status.State = "not_configured"
		} else if !view.Available {
			status.State = "unavailable"
			status.FailureCode = "plugin_registry_unavailable"
		}
	}
	byID := make(map[string]int, len(view.Plugins))
	for _, item := range view.Plugins {
		byID[item.ID] = len(status.Plugins)
		status.Plugins = append(status.Plugins, httpapi.PluginStatusItem{
			ID: item.ID, Type: item.Type, Name: item.Name, AvailableVersion: item.AvailableVersion,
			InstalledVersion: item.InstalledVersion, Installed: item.Installed,
			UpdateAvailable: item.UpdateAvailable, Trust: item.Trust,
		})
	}
	if c.storageCatalog != nil {
		installed, err := c.storageCatalog.Installed()
		if err == nil {
			for _, artifact := range installed {
				if artifact.ID == "local" {
					// storage.local is a mandatory image-bundled provider, not a
					// Registry-managed plugin entry.
					continue
				}
				attestation, trustErr := c.storageAttestation(artifact.ID, artifact.Digest)
				if trustErr != nil {
					// Do not invent Registry approval for an installed executable whose
					// admission record is unavailable. A configured set normally keeps
					// this evidence durable across Registry outages.
					continue
				}
				index, exists := byID[artifact.ID]
				if exists {
					item := &status.Plugins[index]
					item.Type = pluginregistry.TypeStorage
					item.Installed = true
					item.InstalledVersion = artifact.Version
					item.UpdateAvailable = c.pluginRegistry != nil && c.pluginRegistry.UpdateAvailable(artifact.ID, artifact.Version)
					item.StorageState = c.storagePluginLifecycleState(artifact.ID, artifact.Digest)
					item.Trust = attestation
					continue
				}
				name := artifact.ID
				if _, descriptor, describeErr := c.storageCatalog.DescribeArtifact(artifact.Digest); describeErr == nil && descriptor.Name != "" {
					name = descriptor.Name
				}
				status.Plugins = append(status.Plugins, httpapi.PluginStatusItem{
					ID: artifact.ID, Type: pluginregistry.TypeStorage, Name: name,
					InstalledVersion: artifact.Version, Installed: true, StorageState: c.storagePluginLifecycleState(artifact.ID, artifact.Digest), Trust: attestation,
				})
			}
		}
	}
	sort.Slice(status.Plugins, func(i, j int) bool { return status.Plugins[i].ID < status.Plugins[j].ID })
	automatic := c.pluginAutoUpdateStatusSnapshot()
	status.AutomaticUpdate = &automatic
	return status
}

// storagePluginLifecycleState separates an imported executable from a provider
// configuration and from the provider currently pinned by an active Engine.
// It reads only immutable catalog and generation metadata; it never infers an
// activation from a Registry selection or a successful artifact download.
func (c *updateController) storagePluginLifecycleState(id, digest string) string {
	if c == nil || c.storageCatalog == nil || id == "" || digest == "" {
		return "artifact_installed"
	}
	if c.registry != nil {
		snapshot := c.registry.Snapshot()
		for _, item := range snapshot.Generations {
			if item.State != generation.StateActive || item.StorageProviderSetID == "" {
				continue
			}
			set, err := c.storageCatalog.LoadSet(item.StorageProviderSetID)
			if err == nil && set.Artifact.ID == id && set.Artifact.Digest == digest {
				return "active"
			}
		}
	}
	if desired, err := c.storageCatalog.DesiredSet(id); err == nil && desired.Artifact.Digest == digest {
		return "configured"
	}
	instances, err := c.storageCatalog.ListStorageInstances()
	if err == nil {
		for _, instance := range instances {
			if instance.ProviderID != id || instance.DesiredSetID == "" {
				continue
			}
			set, setErr := c.storageCatalog.LoadSet(instance.DesiredSetID)
			if setErr == nil && set.Artifact.Digest == digest {
				return "configured"
			}
		}
	}
	return "artifact_installed"
}

func (c *updateController) pluginAutoUpdateStatusSnapshot() httpapi.PluginAutomaticUpdateStatus {
	if c == nil {
		return httpapi.PluginAutomaticUpdateStatus{State: "idle"}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	status := c.pluginAutoUpdateStatus
	status.LastCheckedAt = cloneTime(status.LastCheckedAt)
	status.LastSuccessAt = cloneTime(status.LastSuccessAt)
	return status
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func (c *updateController) setPluginAutoUpdateState(state, failureCode string, checkedAt, successAt *time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current := c.pluginAutoUpdateStatus
	current.Enabled = c.pluginRegistry != nil && c.pluginRegistry.Configured()
	current.State = state
	current.LastFailureCode = failureCode
	if checkedAt != nil {
		current.LastCheckedAt = cloneTime(checkedAt)
	}
	if successAt != nil {
		current.LastSuccessAt = cloneTime(successAt)
	}
	c.pluginAutoUpdateStatus = current
}

// reconcileRegistryPluginsAutomatically runs one bounded, low-priority pass.
// Refresh and each individual plugin transaction use a non-blocking acquisition
// of the shared Host operation gate, so explicit operations take precedence.
// Source and storage updates deliberately dispatch through their existing
// transaction helpers rather than creating a second installation path.
func (c *updateController) reconcileRegistryPluginsAutomatically(ctx context.Context) (retrySoon bool) {
	if c == nil || ctx == nil || c.pluginRegistry == nil || !c.pluginRegistry.Configured() {
		if c != nil {
			c.setPluginAutoUpdateState("idle", "", nil, nil)
		}
		return false
	}
	if ctx.Err() != nil {
		c.setPluginAutoUpdateState("deferred", "", nil, nil)
		return true
	}
	if c.installation == nil || c.installation.Snapshot().State != installation.StateReady {
		c.setPluginAutoUpdateState("deferred", "", nil, nil)
		return true
	}
	if c.registry == nil || c.registry.Snapshot().StagedGenerationID != "" {
		c.setPluginAutoUpdateState("deferred", "", nil, nil)
		slog.Info("plugin_auto_refresh_deferred", "reason_code", "staged_generation")
		return true
	}
	if err := c.lockOperation(ctx, false); err != nil {
		c.setPluginAutoUpdateState("deferred", "", nil, nil)
		slog.Info("plugin_auto_refresh_deferred", "reason_code", "operation_busy")
		return true
	}
	c.setPluginAutoUpdateState("checking", "", nil, nil)
	slog.Info("plugin_auto_refresh_started")
	refreshErr := c.pluginRegistry.Refresh(ctx)
	c.releaseOperation()
	checkedAt := time.Now().UTC()
	if ctx.Err() != nil {
		c.setPluginAutoUpdateState("deferred", "", nil, nil)
		return true
	}
	if refreshErr != nil {
		slog.Warn("plugin_auto_refresh_failed", "failure_code", "plugin_registry_unavailable")
		c.setPluginAutoUpdateState("failed", "plugin_registry_unavailable", &checkedAt, nil)
		return false
	}

	status := c.pluginStatus()
	candidates := make([]httpapi.PluginStatusItem, 0, len(status.Plugins))
	for _, item := range status.Plugins {
		if item.Installed && item.UpdateAvailable && (item.Type == pluginregistry.TypeSource || item.Type == pluginregistry.TypeStorage) {
			candidates = append(candidates, item)
		}
	}
	for _, candidate := range candidates {
		slog.Info("plugin_auto_update_available", "plugin_id", candidate.ID, "plugin_type", candidate.Type, "from_version", candidate.InstalledVersion, "to_version", candidate.AvailableVersion)
	}
	if len(candidates) > pluginAutoUpdateMaxCandidates {
		candidates = candidates[:pluginAutoUpdateMaxCandidates]
		retrySoon = true
	}
	failureCode := ""
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			c.setPluginAutoUpdateState("deferred", "", &checkedAt, nil)
			return true
		}
		if c.registry.Snapshot().StagedGenerationID != "" {
			c.setPluginAutoUpdateState("deferred", "", &checkedAt, nil)
			slog.Info("plugin_auto_update_deferred", "plugin_id", candidate.ID, "plugin_type", candidate.Type, "reason_code", "staged_generation")
			return true
		}
		// A manual update may have completed after the Registry refresh and
		// before this candidate reached its transaction boundary.
		current := c.pluginStatus()
		if !pluginStatusNeedsUpdate(current, candidate.ID) {
			continue
		}
		if err := c.lockOperation(ctx, false); err != nil {
			c.setPluginAutoUpdateState("deferred", "", &checkedAt, nil)
			slog.Info("plugin_auto_update_deferred", "plugin_id", candidate.ID, "plugin_type", candidate.Type, "reason_code", "operation_busy")
			return true
		}
		c.setPluginAutoUpdateState("updating", "", &checkedAt, nil)
		slog.Info("plugin_auto_update_started", "plugin_id", candidate.ID, "plugin_type", candidate.Type, "from_version", candidate.InstalledVersion, "to_version", candidate.AvailableVersion)
		operationCtx, cancel := context.WithTimeout(ctx, c.operationTimeout)
		var updateErr error
		if candidate.Type == pluginregistry.TypeStorage {
			_, updateErr = c.installStoragePluginLocked(operationCtx, candidate.ID, true)
		} else {
			_, updateErr = c.changePluginLocked(operationCtx, candidate.ID, true)
		}
		cancel()
		c.releaseOperation()
		if ctx.Err() != nil {
			c.setPluginAutoUpdateState("deferred", "", &checkedAt, nil)
			return true
		}
		if updateErr != nil {
			code := pluginAutoUpdateFailureCode(updateErr)
			failureCode = code
			slog.Warn("plugin_auto_update_failed", "plugin_id", candidate.ID, "plugin_type", candidate.Type, "from_version", candidate.InstalledVersion, "to_version", candidate.AvailableVersion, "failure_code", code)
			continue
		}
		if candidate.Type == pluginregistry.TypeStorage {
			artifact, artifactErr := c.installedStorageArtifact(candidate.ID)
			storageState := "artifact_installed"
			if artifactErr == nil {
				storageState = c.storagePluginLifecycleState(candidate.ID, artifact.Digest)
			}
			slog.Info("plugin_auto_update_succeeded", "plugin_id", candidate.ID, "plugin_type", candidate.Type, "from_version", candidate.InstalledVersion, "to_version", candidate.AvailableVersion, "storage_state", storageState, "physical_backend_changed", false)
		} else {
			slog.Info("plugin_auto_update_succeeded", "plugin_id", candidate.ID, "plugin_type", candidate.Type, "from_version", candidate.InstalledVersion, "to_version", candidate.AvailableVersion)
		}
	}
	if failureCode != "" {
		c.setPluginAutoUpdateState("failed", failureCode, &checkedAt, nil)
		return retrySoon
	}
	completedAt := time.Now().UTC()
	c.setPluginAutoUpdateState("idle", "", &checkedAt, &completedAt)
	return retrySoon
}

func pluginStatusNeedsUpdate(status httpapi.PluginStatus, id string) bool {
	for _, item := range status.Plugins {
		if item.ID == id {
			return item.Installed && item.UpdateAvailable
		}
	}
	return false
}

func pluginAutoUpdateFailureCode(err error) string {
	var controllerErr *httpapi.ControllerError
	if errors.As(err, &controllerErr) {
		switch controllerErr.Code {
		case "plugin_registry_unavailable", "plugin_download_failed", "plugin_verification_failed", "plugin_identity_mismatch":
			return controllerErr.Code
		}
	}
	return "plugin_install_failed"
}

func jitterPluginAutoUpdateInterval(interval time.Duration) time.Duration {
	if interval <= 0 {
		return defaultPluginAutoUpdateInterval
	}
	// ±10% avoids synchronizing many Hosts against the same public Registry.
	spread := interval / 10
	if spread <= 0 {
		return interval
	}
	return interval - spread + time.Duration(time.Now().UnixNano()%int64(2*spread+1))
}

func startPeriodicPluginAutoUpdateReconciliation(ctx context.Context, store *installation.Store, controller *updateController, interval time.Duration, jitter func(time.Duration) time.Duration) <-chan struct{} {
	if controller == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return startPeriodicPluginAutoUpdateWorker(ctx, store, controller.pluginAutoUpdateWake, interval, jitter, func(callCtx context.Context) bool {
		callCtx, cancel := context.WithTimeout(callCtx, controller.operationTimeout)
		defer cancel()
		return controller.reconcileRegistryPluginsAutomatically(callCtx)
	})
}

func startPeriodicPluginAutoUpdateWorker(ctx context.Context, store *installation.Store, wake <-chan struct{}, interval time.Duration, jitter func(time.Duration) time.Duration, reconcile func(context.Context) bool) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		if interval <= 0 {
			interval = defaultPluginAutoUpdateInterval
		}
		if jitter == nil {
			jitter = jitterPluginAutoUpdateInterval
		}
		for {
			if store == nil || reconcile == nil {
				return
			}
			if store.Snapshot().State != installation.StateReady {
				if !waitPluginAutoUpdate(ctx, wake, time.Second) {
					return
				}
				continue
			}
			retrySoon := reconcile(ctx)
			delay := jitter(interval)
			if retrySoon && delay > pluginAutoUpdateRetryDelay {
				delay = pluginAutoUpdateRetryDelay
			}
			if !waitPluginAutoUpdate(ctx, wake, delay) {
				return
			}
		}
	}()
	return done
}

func waitPluginAutoUpdate(ctx context.Context, wake <-chan struct{}, delay time.Duration) bool {
	if ctx == nil {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-wake:
		return true
	case <-timer.C:
		return true
	}
}

func (c *updateController) storageAttestation(id, digest string) (plugintrust.Attestation, error) {
	if c == nil || c.storageCatalog == nil || id == "" || digest == "" {
		return plugintrust.Attestation{}, errors.New("storage provider attestation is unavailable")
	}
	installed, err := c.installedStorageArtifact(id)
	if err != nil || installed.Digest != digest {
		return plugintrust.Attestation{}, errors.New("storage provider attestation is unavailable")
	}
	attestation, found, err := c.storageCatalog.InstalledAttestation(id)
	if err != nil {
		return plugintrust.Attestation{}, err
	}
	if found {
		if attestation.Validate() == nil {
			return attestation, nil
		}
		return plugintrust.Attestation{}, errors.New("storage provider attestation is invalid")
	}
	if set, err := c.storageCatalog.DesiredSet(id); err == nil && set.Artifact.Digest == digest {
		attestation := set.EffectiveAttestation()
		if attestation.Provenance == plugintrust.LegacyUnclassified {
			return plugintrust.NewOperator(), nil
		}
		if attestation.Validate() == nil {
			return attestation, nil
		}
		return plugintrust.Attestation{}, errors.New("storage provider attestation is invalid")
	}
	return plugintrust.Attestation{}, errors.New("storage provider attestation is unavailable")
}

func (c *updateController) verifyActivePlugin(selection pluginregistry.DesiredPlugin) error {
	if selection.ID == "" || c.adapterCatalog == nil || c.registry == nil {
		return pluginregistry.ErrIdentityMismatch
	}
	snapshot := c.registry.Snapshot()
	active, ok := snapshot.Generations[snapshot.ActiveGenerationID]
	if !ok || active.State != generation.StateActive || active.AdapterSetID == "" {
		return pluginregistry.ErrIdentityMismatch
	}
	set, err := c.adapterCatalog.Load(active.AdapterSetID)
	if err != nil {
		return pluginregistry.ErrIdentityMismatch
	}
	for _, entry := range set.Entries {
		if entry.AdapterID == selection.ID && entry.Version == selection.Version && entry.ProtocolVersion == adapterproto.Version && entry.ArtifactSHA256 == selection.Digest {
			return nil
		}
	}
	return pluginregistry.ErrIdentityMismatch
}

func (c *updateController) verifyActivePluginAbsent(id string) error {
	if id == "" || c.adapterCatalog == nil || c.registry == nil {
		return pluginregistry.ErrIdentityMismatch
	}
	snapshot := c.registry.Snapshot()
	active, ok := snapshot.Generations[snapshot.ActiveGenerationID]
	if !ok || active.State != generation.StateActive || active.AdapterSetID == "" {
		return pluginregistry.ErrIdentityMismatch
	}
	set, err := c.adapterCatalog.Load(active.AdapterSetID)
	if err != nil {
		return pluginregistry.ErrIdentityMismatch
	}
	for _, entry := range set.Entries {
		if entry.AdapterID == id {
			return pluginregistry.ErrIdentityMismatch
		}
	}
	return nil
}

func mapPluginRegistryError(err error) error {
	switch {
	case errors.Is(err, pluginregistry.ErrPluginNotFound), errors.Is(err, pluginregistry.ErrNotInstalled):
		return httpapi.NewControllerError("plugin_not_found")
	case errors.Is(err, pluginregistry.ErrPlatformUnsupported):
		return httpapi.NewControllerError("plugin_platform_unsupported")
	case errors.Is(err, pluginregistry.ErrDownloadFailed):
		return httpapi.NewControllerError("plugin_download_failed")
	case errors.Is(err, pluginregistry.ErrVerificationFailed):
		return httpapi.NewControllerError("plugin_verification_failed")
	case errors.Is(err, pluginregistry.ErrIdentityMismatch):
		return httpapi.NewControllerError("plugin_identity_mismatch")
	case errors.Is(err, pluginregistry.ErrWrongPluginType):
		return httpapi.NewControllerError("plugin_type_mismatch")
	case errors.Is(err, pluginregistry.ErrUnavailable):
		return httpapi.NewControllerError("plugin_registry_unavailable")
	case errors.Is(err, pluginregistry.ErrAlreadyInstalled), errors.Is(err, pluginregistry.ErrNoUpdateAvailable), errors.Is(err, pluginregistry.ErrOperationConflict):
		return httpapi.NewControllerError("plugin_operation_conflict")
	default:
		return httpapi.NewControllerError("plugin_install_failed")
	}
}
