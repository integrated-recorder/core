package bootstrap

import (
	"context"
	"errors"
	"sort"

	"github.com/dltkddnr04/integrated-recorder/internal/adapterproto"
	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/adaptercatalog"
	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/generation"
	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/httpapi"
	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/installation"
	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/pluginregistry"
	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/storagecatalog"
	"github.com/dltkddnr04/integrated-recorder/internal/storageproto"
)

type sourceAwareAdapterCatalog interface {
	ReconcileWithSources(context.Context, string, []string) (adaptercatalog.Snapshot, error)
}

func (c *updateController) reconcileCatalog(ctx context.Context, fallbackSetID string) (adaptercatalog.Snapshot, error) {
	var sources []string
	if c.pluginRegistry != nil {
		var err error
		sources, err = c.pluginRegistry.DesiredSourceDirs()
		if err != nil {
			return adaptercatalog.Snapshot{}, err
		}
	}
	if len(sources) == 0 {
		return c.adapterCatalog.Reconcile(ctx, fallbackSetID)
	}
	withSources, ok := c.adapterCatalog.(sourceAwareAdapterCatalog)
	if !ok {
		return adaptercatalog.Snapshot{}, errors.New("adapter catalog does not support Host-owned plugin sources")
	}
	return withSources.ReconcileWithSources(ctx, fallbackSetID, sources)
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
			next, setErr := c.storageCatalog.CreateSet(artifact.Digest, *previousConfig)
			if setErr == nil {
				migratedSet = &next
			} else if !errors.Is(setErr, storagecatalog.ErrInvalidConfig) {
				return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_install_failed")
			}
		}
	}
	if err := c.storageCatalog.Install(artifact); err != nil {
		return httpapi.PluginStatus{}, httpapi.NewControllerError("plugin_install_failed")
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
			UpdateAvailable: item.UpdateAvailable,
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
				index, exists := byID[artifact.ID]
				if exists {
					item := &status.Plugins[index]
					item.Type = pluginregistry.TypeStorage
					item.Installed = true
					item.InstalledVersion = artifact.Version
					item.UpdateAvailable = c.pluginRegistry != nil && c.pluginRegistry.UpdateAvailable(artifact.ID, artifact.Version)
					continue
				}
				name := artifact.ID
				if _, descriptor, describeErr := c.storageCatalog.DescribeArtifact(artifact.Digest); describeErr == nil && descriptor.Name != "" {
					name = descriptor.Name
				}
				status.Plugins = append(status.Plugins, httpapi.PluginStatusItem{
					ID: artifact.ID, Type: pluginregistry.TypeStorage, Name: name,
					InstalledVersion: artifact.Version, Installed: true,
				})
			}
		}
	}
	sort.Slice(status.Plugins, func(i, j int) bool { return status.Plugins[i].ID < status.Plugins[j].ID })
	return status
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
