package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/generation"
	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/httpapi"
	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/installation"
	"github.com/dltkddnr04/integrated-recorder/internal/runtimehost/storagecatalog"
	"github.com/dltkddnr04/integrated-recorder/internal/storage"
	"github.com/dltkddnr04/integrated-recorder/internal/storageprocess"
	"github.com/dltkddnr04/integrated-recorder/internal/storageproto"
)

var _ httpapi.StorageController = (*updateController)(nil)

func ensureBundledLocalStorage(ctx context.Context, catalog *storagecatalog.Catalog, binary, archiveRoot string) (storagecatalog.Set, error) {
	if ctx == nil || ctx.Err() != nil || catalog == nil || !filepath.IsAbs(binary) || filepath.Clean(binary) != binary ||
		!filepath.IsAbs(archiveRoot) || filepath.Clean(archiveRoot) != archiveRoot {
		return storagecatalog.Set{}, errors.New("bundled local storage configuration is invalid")
	}
	artifact, err := catalog.ImportBundled(ctx, binary, "local")
	if err != nil {
		return storagecatalog.Set{}, errors.New("bundled storage.local artifact failed validation")
	}
	if err := catalog.Install(artifact); err != nil {
		return storagecatalog.Set{}, errors.New("bundled storage.local artifact could not be installed")
	}
	rootValue, err := json.Marshal(archiveRoot)
	if err != nil {
		return storagecatalog.Set{}, errors.New("bundled storage.local configuration is invalid")
	}
	set, err := catalog.CreateSet(artifact.Digest, storagecatalog.SetConfig{
		Values: map[string]json.RawMessage{"root": rootValue},
	})
	if err != nil {
		return storagecatalog.Set{}, errors.New("bundled storage.local set could not be created")
	}
	if err := catalog.SelectDesiredSet("local", set.ID); err != nil {
		return storagecatalog.Set{}, errors.New("bundled storage.local set could not be selected")
	}
	return set, nil
}

func probeStorageProviderSet(ctx context.Context, catalogRoot, setID string) error {
	if ctx == nil || ctx.Err() != nil || setID == "" {
		return errors.New("storage provider probe could not start")
	}
	provider, err := storageprocess.StartSet(ctx, catalogRoot, setID)
	if err != nil {
		return errors.New("storage provider process probe failed")
	}
	closed := false
	defer func() {
		if !closed {
			_ = provider.Close()
		}
	}()
	if err := storage.ProbePhysicalObjectStore(ctx, provider.ObjectStore()); err != nil {
		return errors.New("storage provider object probe failed")
	}
	if err := provider.Close(); err != nil {
		return errors.New("storage provider process cleanup failed")
	}
	closed = true
	return nil
}

func (c *updateController) StorageStatus(ctx context.Context) (httpapi.StorageProviderStatus, error) {
	if ctx == nil || ctx.Err() != nil || c.registry == nil {
		return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("internal_error")
	}
	snapshot := c.registry.Snapshot()
	active, exists := snapshot.Generations[snapshot.ActiveGenerationID]
	if !exists || active.State != generation.StateActive {
		return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_provider_unavailable")
	}
	status := httpapi.StorageProviderStatus{
		Primary:   httpapi.PrimaryStorageStatus{Kind: "plugin", ProviderID: "local", State: "ready"},
		Providers: []httpapi.StorageProviderSummary{},
	}
	activeSet := storagecatalog.Set{}
	if c.storageCatalog == nil || active.StorageProviderSetID == "" {
		return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_provider_unavailable")
	}
	set, err := c.storageCatalog.LoadSet(active.StorageProviderSetID)
	if err != nil {
		return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_provider_unavailable")
	}
	activeSet = set
	status.Primary = httpapi.PrimaryStorageStatus{Kind: "plugin", ProviderID: set.Artifact.ID, Version: set.Artifact.Version, State: "ready"}
	installed, err := c.storageCatalog.Installed()
	if err != nil {
		return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_provider_unavailable")
	}
	byID := make(map[string]storagecatalog.Artifact, len(installed)+1)
	for _, artifact := range installed {
		byID[artifact.ID] = artifact
	}
	// An active or draining generation may pin an artifact that is no longer
	// the installed default. Keep it visible only when there is no current
	// installed artifact for that provider; never let an older generation
	// overwrite the installed version in the inventory projection.
	if activeSet.ID != "" {
		if _, exists := byID[activeSet.Artifact.ID]; !exists {
			byID[activeSet.Artifact.ID] = activeSet.Artifact
		}
	}
	generationIDs := make([]string, 0, len(snapshot.Generations))
	for id, item := range snapshot.Generations {
		if item.State == generation.StateActive || item.State == generation.StateDraining {
			generationIDs = append(generationIDs, id)
		}
	}
	sort.Strings(generationIDs)
	for _, id := range generationIDs {
		item := snapshot.Generations[id]
		if item.StorageProviderSetID == "" || id == active.ID {
			continue
		}
		set, setErr := c.storageCatalog.LoadSet(item.StorageProviderSetID)
		if setErr != nil {
			return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_provider_unavailable")
		}
		if _, exists := byID[set.Artifact.ID]; !exists {
			byID[set.Artifact.ID] = set.Artifact
		}
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		artifact := byID[id]
		_, descriptor, describeErr := c.storageCatalog.DescribeArtifact(artifact.Digest)
		if describeErr != nil {
			return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_provider_unavailable")
		}
		configured := false
		providerSet, setErr := c.storageCatalog.DesiredSet(id)
		if setErr == nil {
			configured = providerSet.Artifact.Digest == artifact.Digest
		} else if !errors.Is(setErr, storagecatalog.ErrSetMissing) {
			return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_provider_unavailable")
		}
		activeProvider := activeSet.ID != "" && activeSet.Artifact.ID == id && activeSet.Artifact.Digest == artifact.Digest
		health := "unknown"
		if activeProvider {
			health = "ready" // Engine and Control readiness require a successful provider Probe.
		}
		summary := httpapi.StorageProviderSummary{
			ID: id, Name: descriptor.Name, Version: artifact.Version, Configured: configured,
			Active: activeProvider, Health: health, ConfigurationSchema: descriptor.ConfigurationSchema,
			Distribution: "registry", Uninstallable: true,
		}
		if id == "local" {
			summary.Distribution = "bundled"
			summary.ConfigurationManaged = true
			summary.Uninstallable = false
			// The provider configuration contains the Host-selected archive root.
			// Do not publish even its schema in a public response.
			summary.ConfigurationSchema = storageproto.Schema{Fields: []storageproto.Field{}}
		}
		status.Providers = append(status.Providers, summary)
	}
	return status, nil
}

func (c *updateController) StorageConfig(ctx context.Context, id string) (httpapi.StorageConfigView, error) {
	if id == "local" {
		return httpapi.StorageConfigView{Values: map[string]json.RawMessage{}, ConfiguredSecrets: []string{}}, nil
	}
	if ctx == nil || ctx.Err() != nil || c.storageCatalog == nil || !pluginregistryIDValid(id) {
		return httpapi.StorageConfigView{}, httpapi.NewControllerError("storage_provider_not_installed")
	}
	installed, err := c.installedStorageArtifact(id)
	if errors.Is(err, storagecatalog.ErrArtifactMissing) {
		return httpapi.StorageConfigView{}, httpapi.NewControllerError("storage_provider_not_installed")
	}
	if err != nil {
		return httpapi.StorageConfigView{}, httpapi.NewControllerError("storage_provider_unavailable")
	}
	_, descriptor, err := c.storageCatalog.DescribeArtifact(installed.Digest)
	if err != nil {
		return httpapi.StorageConfigView{}, httpapi.NewControllerError("storage_provider_unavailable")
	}
	set, err := c.storageCatalog.DesiredSet(id)
	if errors.Is(err, storagecatalog.ErrSetMissing) {
		return storageConfigView(storagecatalog.SetConfig{}, descriptor.ConfigurationSchema), nil
	}
	if err != nil {
		return httpapi.StorageConfigView{}, httpapi.NewControllerError("storage_provider_unavailable")
	}
	if set.Artifact.Digest != installed.Digest {
		// A prior immutable set may still be pinned by an active or draining
		// generation, but its values and secret-presence indicators belong to
		// that old descriptor and must not be projected as current configuration.
		return storageConfigView(storagecatalog.SetConfig{}, descriptor.ConfigurationSchema), nil
	}
	return storageConfigView(set.Config, descriptor.ConfigurationSchema), nil
}

func (c *updateController) ConfigureStorage(ctx context.Context, id string, request httpapi.StorageConfigRequest) (httpapi.StorageConfigView, error) {
	if id == "local" {
		return httpapi.StorageConfigView{}, httpapi.NewControllerError("storage_provider_config_managed")
	}
	operationCtx, cancel, err := c.beginStorageOperation(ctx)
	if err != nil {
		return httpapi.StorageConfigView{}, err
	}
	defer cancel()
	defer c.releaseOperation()
	if c.storageCatalog == nil || !pluginregistryIDValid(id) {
		return httpapi.StorageConfigView{}, httpapi.NewControllerError("storage_provider_not_installed")
	}
	artifact, err := c.installedStorageArtifact(id)
	if err != nil {
		return httpapi.StorageConfigView{}, httpapi.NewControllerError("storage_provider_not_installed")
	}
	_, descriptor, err := c.storageCatalog.DescribeArtifact(artifact.Digest)
	if err != nil {
		return httpapi.StorageConfigView{}, httpapi.NewControllerError("storage_provider_unavailable")
	}
	config := storagecatalog.SetConfig{Values: cloneRawValues(request.Values), Secrets: cloneSecretValues(request.Secrets)}
	if old, oldErr := c.storageCatalog.DesiredSet(id); oldErr == nil {
		if old.Artifact.Digest == artifact.Digest {
			// The UI never reads a secret back. Leaving an existing secret field
			// blank preserves it only when it belongs to this exact immutable
			// artifact. A stale generation's secret must not cross a provider update.
			for _, field := range descriptor.ConfigurationSchema.Fields {
				if field.Control == "secret" && config.Secrets[field.Key] == "" && old.Config.Secrets[field.Key] != "" {
					config.Secrets[field.Key] = old.Config.Secrets[field.Key]
				}
			}
		}
	} else if !errors.Is(oldErr, storagecatalog.ErrSetMissing) {
		return httpapi.StorageConfigView{}, httpapi.NewControllerError("storage_provider_unavailable")
	}
	if err := operationCtx.Err(); err != nil {
		return httpapi.StorageConfigView{}, httpapi.NewControllerError("storage_operation_conflict")
	}
	set, err := c.storageCatalog.CreateSet(artifact.Digest, config)
	if err != nil {
		return httpapi.StorageConfigView{}, httpapi.NewControllerError("storage_provider_not_configured")
	}
	if err := c.storageCatalog.SelectDesiredSet(id, set.ID); err != nil {
		return httpapi.StorageConfigView{}, httpapi.NewControllerError("storage_provider_not_configured")
	}
	return storageConfigView(set.Config, descriptor.ConfigurationSchema), nil
}

func (c *updateController) ProbeStorage(ctx context.Context, id string) (httpapi.StorageProbeResult, error) {
	operationCtx, cancel, err := c.beginStorageOperation(ctx)
	if err != nil {
		return httpapi.StorageProbeResult{}, err
	}
	defer cancel()
	defer c.releaseOperation()
	if c.storageCatalog == nil || !pluginregistryIDValid(id) {
		return httpapi.StorageProbeResult{}, httpapi.NewControllerError("storage_provider_not_installed")
	}
	set, err := c.currentDesiredStorageSet(id)
	if err != nil {
		return httpapi.StorageProbeResult{}, err
	}
	if err := c.probeStorageProvider(operationCtx, set.ID); err != nil {
		return httpapi.StorageProbeResult{}, httpapi.NewControllerError("storage_provider_probe_failed")
	}
	return httpapi.StorageProbeResult{State: "ready"}, nil
}

func (c *updateController) ActivateStorage(ctx context.Context, id string) (httpapi.StorageProviderStatus, error) {
	operationCtx, cancel, err := c.beginStorageOperation(ctx)
	if err != nil {
		return httpapi.StorageProviderStatus{}, err
	}
	defer cancel()
	defer c.releaseOperation()
	if c.registry == nil || !pluginregistryIDValid(id) {
		return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_provider_not_installed")
	}
	snapshot := c.registry.Snapshot()
	active, exists := snapshot.Generations[snapshot.ActiveGenerationID]
	if !exists || active.State != generation.StateActive {
		return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_provider_unavailable")
	}
	if snapshot.StagedGenerationID != "" {
		return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_operation_conflict")
	}
	if c.storageCatalog == nil {
		return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_provider_not_installed")
	}
	set, err := c.currentDesiredStorageSet(id)
	if err != nil {
		return httpapi.StorageProviderStatus{}, err
	}
	if err := c.probeStorageProvider(operationCtx, set.ID); err != nil {
		return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_provider_probe_failed")
	}
	if active.StorageProviderSetID == set.ID {
		return c.StorageStatus(operationCtx)
	}
	samePhysicalBackend := samePhysicalStorageIdentity(c.storageCatalog, active.StorageProviderSetID, set)
	if !samePhysicalBackend {
		if len(snapshot.Leases) > 0 {
			return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_backend_in_use")
		}
		activeEmpty, err := c.archiveEmpty(operationCtx, active.StorageProviderSetID)
		if err != nil {
			return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_provider_unavailable")
		}
		candidateEmpty, err := c.archiveEmpty(operationCtx, set.ID)
		if err != nil {
			return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_provider_unavailable")
		}
		if !activeEmpty || !candidateEmpty {
			return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_backend_switch_requires_empty_archive")
		}
	}
	if err := c.activateStorageGeneration(operationCtx, active, set.ID); err != nil {
		return httpapi.StorageProviderStatus{}, httpapi.NewControllerError("storage_activation_failed")
	}
	return c.StorageStatus(operationCtx)
}

func (c *updateController) probeStorageProvider(ctx context.Context, setID string) error {
	return probeStorageProviderSet(ctx, filepath.Join(c.config.DataDir, "runtime", "storage-providers"), setID)
}

func (c *updateController) beginStorageOperation(ctx context.Context) (context.Context, context.CancelFunc, error) {
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
		return nil, nil, httpapi.NewControllerError("storage_operation_conflict")
	}
	if operationCtx.Err() != nil {
		c.releaseOperation()
		cancel()
		return nil, nil, httpapi.NewControllerError("storage_operation_conflict")
	}
	return operationCtx, cancel, nil
}

func (c *updateController) installedStorageArtifact(id string) (storagecatalog.Artifact, error) {
	items, err := c.storageCatalog.Installed()
	if err != nil {
		return storagecatalog.Artifact{}, err
	}
	for _, item := range items {
		if item.ID == id {
			return item, nil
		}
	}
	return storagecatalog.Artifact{}, storagecatalog.ErrArtifactMissing
}

// currentDesiredStorageSet requires configuration to pin the currently
// installed artifact. An older set may remain referenced by a generation,
// but it is not eligible for a new probe or activation after an update.
func (c *updateController) currentDesiredStorageSet(id string) (storagecatalog.Set, error) {
	installed, err := c.installedStorageArtifact(id)
	if errors.Is(err, storagecatalog.ErrArtifactMissing) {
		return storagecatalog.Set{}, httpapi.NewControllerError("storage_provider_not_installed")
	}
	if err != nil {
		return storagecatalog.Set{}, httpapi.NewControllerError("storage_provider_unavailable")
	}
	set, err := c.storageCatalog.DesiredSet(id)
	if errors.Is(err, storagecatalog.ErrSetMissing) {
		return storagecatalog.Set{}, httpapi.NewControllerError("storage_provider_not_configured")
	}
	if err != nil {
		return storagecatalog.Set{}, httpapi.NewControllerError("storage_provider_unavailable")
	}
	if set.Artifact.Digest != installed.Digest {
		return storagecatalog.Set{}, httpapi.NewControllerError("storage_provider_not_configured")
	}
	return set, nil
}

func storageConfigView(config storagecatalog.SetConfig, schema storageproto.Schema) httpapi.StorageConfigView {
	view := httpapi.StorageConfigView{Values: cloneRawValues(config.Values), ConfiguredSecrets: []string{}}
	for _, field := range schema.Fields {
		if field.Control == "secret" && config.Secrets[field.Key] != "" {
			view.ConfiguredSecrets = append(view.ConfiguredSecrets, field.Key)
		}
	}
	sort.Strings(view.ConfiguredSecrets)
	return view
}

func cloneRawValues(source map[string]json.RawMessage) map[string]json.RawMessage {
	clone := make(map[string]json.RawMessage, len(source))
	for key, value := range source {
		clone[key] = append(json.RawMessage(nil), value...)
	}
	return clone
}

func cloneSecretValues(source map[string]string) map[string]string {
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func samePhysicalStorageIdentity(catalog *storagecatalog.Catalog, activeSetID string, candidate storagecatalog.Set) bool {
	if activeSetID == "" || catalog == nil {
		return false
	}
	active, err := catalog.LoadSet(activeSetID)
	if err != nil || active.Artifact.ID != candidate.Artifact.ID {
		return false
	}
	return reflect.DeepEqual(active.Config.Values, candidate.Config.Values) && reflect.DeepEqual(active.Config.Secrets, candidate.Config.Secrets)
}

func (c *updateController) archiveEmpty(ctx context.Context, storageSetID string) (bool, error) {
	// Core stages bounded payloads on shared local disk before streaming them to
	// any physical backend. Backend emptiness alone therefore cannot authorize
	// a switch while staged canonical data may still be pending.
	stagingEntries, err := readDirIfPresent(filepath.Join(c.config.DataDir, "runtime", "storage-staging"))
	if err != nil {
		return false, err
	}
	if len(stagingEntries) != 0 {
		return false, nil
	}
	if storageSetID == "" {
		return false, errors.New("storage provider generation identity is unavailable")
	}
	provider, err := storageprocess.StartSet(ctx, filepath.Join(c.config.DataDir, "runtime", "storage-providers"), storageSetID)
	if err != nil {
		return false, err
	}
	defer provider.Close()
	for _, prefix := range []string{"recordings/", "recording-deletions/"} {
		page, err := provider.ObjectStore().List(ctx, prefix, "", 1)
		if err != nil {
			return false, err
		}
		if len(page.Items) > 0 {
			return false, nil
		}
	}
	return true, nil
}

func readDirIfPresent(path string) ([]os.DirEntry, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("archive state is unavailable")
	}
	return os.ReadDir(path)
}

func (c *updateController) activateStorageGeneration(ctx context.Context, active generation.Generation, storageSetID string) error {
	if ctx == nil || !validStorageProviderSetIdentity(storageSetID) {
		return errors.New("storage generation identity is invalid")
	}
	id, err := newGenerationID()
	if err != nil {
		return err
	}
	candidate := active
	candidate.ID = id
	candidate.StorageProviderSetID = storageSetID
	candidate.InstalledAt = time.Now().UTC()
	candidate.State = generation.StateStaging
	candidate.EngineDormant = false
	if err := c.registry.Stage(candidate); err != nil {
		return err
	}
	if err := c.registry.MarkVerified(id); err != nil {
		_ = c.registry.Fail(id)
		return err
	}
	directory, manifest, paths, err := c.applicationFiles(active)
	if err != nil {
		c.failCandidate(ctx, id)
		return err
	}
	operationCtx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()
	attachment, err := c.startEngine(operationCtx, id, directory, manifest, paths)
	if err == nil {
		err = c.registerEngine(id, attachment)
	}
	if err == nil {
		err = c.publishControl(operationCtx, id, directory, manifest, paths, id)
	}
	if err == nil {
		err = c.registry.MarkReady(id)
	}
	if err != nil {
		c.failCandidate(operationCtx, id)
		return err
	}
	previousID := active.ID
	activationErr := c.supervisor.ActivateControlWith(operationCtx, id,
		func() error { return c.registry.Activate(id) },
		func() error { return c.registry.AbortActivation(id, previousID) },
	)
	current := c.registry.Snapshot()
	if current.ActiveGenerationID != id || c.supervisor.Snapshot().ActiveControlGeneration != id {
		c.failCandidate(operationCtx, id)
		return activationErrOrCandidate(activationErr)
	}
	if err := c.registry.FinalizeActivation(id); err != nil {
		// Startup reconciles the durable activation journal idempotently.
	}
	c.mu.Lock()
	if manifest.ReleaseVersion != "" {
		c.manifests[id] = manifest
		c.buildIdentities[id] = buildInfoFromManifest(manifest)
	}
	c.mu.Unlock()
	return nil
}

func activationErrOrCandidate(err error) error {
	if err != nil {
		return err
	}
	return errors.New("storage generation activation did not commit")
}

func pluginregistryIDValid(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for i, character := range id {
		if !(character >= 'a' && character <= 'z') && !(character >= '0' && character <= '9') && character != '.' && character != '_' && character != '-' || i == 0 && !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9') {
			return false
		}
	}
	return true
}
