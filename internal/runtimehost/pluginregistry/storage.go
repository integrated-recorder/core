package pluginregistry

import (
	"context"
	"os"
	"path/filepath"
	"sync"
)

// StorageArtifactPlan holds the Plugin Registry operation gate while a
// registry-approved storage-provider executable is imported into the
// Runtime Host's immutable storage catalog. It deliberately does not change
// source-plugin desired state and does not activate a storage backend.
type StorageArtifactPlan struct {
	manager   *Manager
	selection DesiredPlugin
	path      string
	once      sync.Once
}

func (p *StorageArtifactPlan) Selection() DesiredPlugin {
	if p == nil {
		return DesiredPlugin{}
	}
	selection := p.selection
	if p.selection.Attestation != nil {
		attestation := *p.selection.Attestation
		selection.Attestation = &attestation
	}
	return selection
}

// BinaryPath is private Host orchestration input. Do not project it through
// public API responses or expose it to application generations.
func (p *StorageArtifactPlan) BinaryPath() string {
	if p == nil {
		return ""
	}
	return p.path
}

func (p *StorageArtifactPlan) Close() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		if p.manager != nil {
			p.manager.release()
		}
	})
}

// PrepareStorageArtifact downloads and verifies an explicitly typed storage
// provider release without making it desired or active. The executable still
// has to be imported into storagecatalog and configured/probed before a new
// application generation may select it.
func (m *Manager) PrepareStorageArtifact(ctx context.Context, id string) (*StorageArtifactPlan, error) {
	if m == nil || ctx == nil || !validPluginID(id) {
		return nil, ErrPluginNotFound
	}
	if err := m.acquire(ctx); err != nil {
		return nil, ErrUnavailable
	}
	locked := true
	defer func() {
		if locked {
			m.release()
		}
	}()
	m.stateMu.RLock()
	configured, available := m.registryURL != "", m.available
	authority := m.registryAuthority
	var document *registryDocument
	if m.registry != nil {
		copy := *m.registry
		document = &copy
	}
	m.stateMu.RUnlock()
	if !configured || !available || document == nil {
		return nil, ErrUnavailable
	}
	plugin, ok := findPlugin(document.Plugins, id)
	if !ok {
		return nil, ErrPluginNotFound
	}
	pluginType := plugin.Type
	if document.SchemaVersion == SchemaVersion {
		pluginType = TypeSource
	}
	if pluginType != TypeStorage {
		return nil, ErrWrongPluginType
	}
	release, ok := stableRelease(plugin)
	if !ok {
		return nil, ErrNoUpdateAvailable
	}
	artifact, ok := selectArtifact(release, m.goos, m.goarch)
	if !ok {
		return nil, ErrPlatformUnsupported
	}
	if err := m.downloadAndVerify(ctx, plugin, release, artifact); err != nil {
		return nil, redactFailure(err)
	}
	selection := DesiredPlugin{ID: plugin.ID, Name: plugin.Name, Version: release.Version, Channel: "stable", Digest: artifact.SHA256, Filename: artifact.Filename, Size: artifact.Size, SourceCommit: release.SourceCommit, Attestation: attestationPointer(registryAttestation(document.SchemaVersion, authority, plugin.Publisher))}
	path := filepath.Join(m.root, "artifacts", artifact.SHA256)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() != artifact.Size || info.Mode().Perm()&0077 != 0 {
		return nil, ErrUnsafeStore
	}
	locked = false
	return &StorageArtifactPlan{manager: m, selection: selection, path: path}, nil
}
