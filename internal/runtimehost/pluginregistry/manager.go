package pluginregistry

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/network"
	"github.com/integrated-recorder/core/internal/storageproto"
)

const (
	requestTimeout = 30 * time.Second
	maxRedirects   = 3
	copyBufferSize = 128 << 10
)

var ErrWrongPluginType = errors.New("plugin type is not supported by the source adapter installer")

type Config struct {
	Root        string
	RegistryURL string
	GOOS        string
	GOARCH      string
	HTTPClient  *http.Client
}

type Manager struct {
	root        string
	registryURL string
	goos        string
	goarch      string
	client      *http.Client
	injected    bool
	gate        chan struct{}
	stateMu     sync.RWMutex
	desired     desiredState
	registry    *registryDocument
	available   bool
	failureCode string
}

func Open(config Config) (*Manager, error) {
	if !filepath.IsAbs(config.Root) || filepath.Clean(config.Root) != config.Root || config.Root == string(filepath.Separator) {
		return nil, ErrInvalidConfig
	}
	if config.GOOS == "" {
		config.GOOS = runtime.GOOS
	}
	if config.GOARCH == "" {
		config.GOARCH = runtime.GOARCH
	}
	if config.RegistryURL != "" && !validHTTPSURL(config.RegistryURL, false) {
		return nil, ErrInvalidConfig
	}
	if config.HTTPClient == nil && config.RegistryURL != "" {
		u, _ := url.Parse(config.RegistryURL)
		if u.Scheme != "https" || u.User != nil || u.Fragment != "" || u.RawQuery != "" {
			return nil, ErrInvalidConfig
		}
	}
	client, injected := configureClient(config.HTTPClient)
	m := &Manager{
		root: config.Root, registryURL: config.RegistryURL,
		goos: config.GOOS, goarch: config.GOARCH, client: client,
		injected: injected, gate: make(chan struct{}, 1),
		failureCode: "registry_unavailable",
	}
	if err := m.ensureLayout(); err != nil {
		return nil, ErrUnsafeStore
	}
	state, err := m.loadDesired()
	if err != nil {
		return nil, ErrUnsafeStore
	}
	m.desired = state
	// Once the durable desired snapshot has been validated, only its source set
	// and artifacts remain needed by this store. Adaptercatalog imports its own
	// immutable copies, so orphan publications from a crashed pre-Commit plan
	// can be collected before the Host starts generation reconciliation.
	if err := m.CollectGarbage(); err != nil {
		return nil, ErrUnsafeStore
	}
	return m, nil
}

// configured is represented separately from endpoint availability so the
// empty-registry case remains safe and visible to callers.
func (m *Manager) Configured() bool { return m != nil && m.registryURL != "" }

func configureClient(injected *http.Client) (*http.Client, bool) {
	base := injected
	if base == nil {
		base = network.NewPublicHTTPClient(requestTimeout)
	}
	copy := *base
	if copy.Timeout <= 0 || copy.Timeout > requestTimeout {
		copy.Timeout = requestTimeout
	}
	original := base.CheckRedirect
	copy.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) > maxRedirects || req.URL == nil || req.URL.Scheme != "https" || req.URL.Host == "" || req.URL.User != nil || req.URL.Fragment != "" {
			return fmt.Errorf("redirect rejected")
		}
		if len(via) == 0 {
			return fmt.Errorf("redirect rejected")
		}
		initial := via[0].URL
		if injected != nil && localTestOrigin(initial) {
			// Local TLS fixtures are the only private destinations the injected
			// test client may reach, and redirects must remain on that exact origin.
			if !sameURLOrigin(initial, req.URL) || !localTestOrigin(req.URL) {
				return fmt.Errorf("redirect rejected")
			}
		} else if err := network.ValidatePublicURL(req.Context(), req.URL.String()); err != nil {
			return fmt.Errorf("redirect rejected")
		}
		if original != nil {
			if err := original(req, via); err != nil {
				return err
			}
		}
		return nil
	}
	return &copy, injected != nil
}

func localTestOrigin(u *url.URL) bool {
	if u == nil || u.Scheme != "https" || u.User != nil || u.Fragment != "" || !validHostPort(u) {
		return false
	}
	host := u.Hostname()
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func sameURLOrigin(left, right *url.URL) bool {
	return left != nil && right != nil && strings.EqualFold(left.Scheme, right.Scheme) && strings.EqualFold(left.Host, right.Host)
}

func validateNetworkURL(ctx context.Context, rawURL string, injected bool) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ErrUnavailable
	}
	if injected && localTestOrigin(u) {
		return nil
	}
	if err := network.ValidatePublicURL(ctx, rawURL); err != nil {
		return ErrUnavailable
	}
	return nil
}

func (m *Manager) acquire(ctx context.Context) error {
	select {
	case m.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) release() { <-m.gate }

func (m *Manager) Refresh(ctx context.Context) error {
	if m == nil || ctx == nil {
		return ErrInvalidConfig
	}
	if err := m.acquire(ctx); err != nil {
		return ErrUnavailable
	}
	defer m.release()
	if m.registryURL == "" {
		m.setUnavailable()
		return ErrUnavailable
	}
	data, err := m.getBounded(ctx, m.registryURL, MaxCatalogBytes)
	if err != nil {
		m.setUnavailable()
		return ErrUnavailable
	}
	document, err := decodeRegistry(data)
	if err != nil {
		m.setUnavailable()
		return ErrUnavailable
	}
	m.stateMu.Lock()
	m.registry = &document
	m.available = true
	m.failureCode = ""
	m.stateMu.Unlock()
	return nil
}

func (m *Manager) setUnavailable() {
	m.stateMu.Lock()
	m.available = false
	m.failureCode = "registry_unavailable"
	m.stateMu.Unlock()
}

func (m *Manager) View() View {
	if m == nil {
		return View{FailureCode: "registry_unavailable"}
	}
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	view := View{Configured: m.registryURL != "", Available: m.available, FailureCode: m.failureCode, Plugins: []PluginView{}, Installed: cloneDesired(m.desired.Plugins)}
	installed := make(map[string]DesiredPlugin, len(m.desired.Plugins))
	for _, item := range m.desired.Plugins {
		installed[item.ID] = item
	}
	seen := map[string]bool{}
	if m.available && m.registry != nil {
		plugins := append([]registryPlugin(nil), m.registry.Plugins...)
		sort.Slice(plugins, func(i, j int) bool { return plugins[i].ID < plugins[j].ID })
		for _, plugin := range plugins {
			release, hasStable := stableRelease(plugin)
			item, isInstalled := installed[plugin.ID]
			pluginType := plugin.Type
			if m.registry.SchemaVersion == SchemaVersion {
				pluginType = TypeSource
			}
			if pluginType == "" {
				pluginType = TypeSource
			}
			p := PluginView{ID: plugin.ID, Name: plugin.Name, Type: pluginType, Installed: isInstalled}
			if hasStable {
				p.AvailableVersion = release.Version
			}
			if isInstalled {
				p.InstalledVersion = item.Version
				if hasStable {
					_, hasArtifact := selectArtifact(release, m.goos, m.goarch)
					p.UpdateAvailable = hasArtifact && item.Version != release.Version
				}
			}
			view.Plugins = append(view.Plugins, p)
			seen[plugin.ID] = true
		}
	}
	for _, item := range m.desired.Plugins {
		if seen[item.ID] {
			continue
		}
		view.Plugins = append(view.Plugins, PluginView{ID: item.ID, Name: item.Name, Type: TypeSource, InstalledVersion: item.Version, Installed: true})
	}
	sort.Slice(view.Plugins, func(i, j int) bool { return view.Plugins[i].ID < view.Plugins[j].ID })
	return view
}

// UpdateAvailable reports whether the currently approved registry snapshot
// has a stable artifact for the running platform whose version differs from
// the installed typed plugin version.
func (m *Manager) UpdateAvailable(id, installedVersion string) bool {
	if m == nil || !validPluginID(id) || installedVersion == "" {
		return false
	}
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	if !m.available || m.registry == nil {
		return false
	}
	plugin, ok := findPlugin(m.registry.Plugins, id)
	if !ok {
		return false
	}
	release, ok := stableRelease(plugin)
	if !ok || release.Version == installedVersion {
		return false
	}
	_, ok = selectArtifact(release, m.goos, m.goarch)
	return ok
}

func (m *Manager) DesiredSourceDirs() ([]string, error) {
	if m == nil {
		return nil, ErrInvalidConfig
	}
	state, err := m.loadDesired()
	if err != nil {
		return nil, ErrUnsafeStore
	}
	return []string{safeSetDirectoryPath(m.root, state.SourceSetID)}, nil
}

func (m *Manager) PrepareInstall(ctx context.Context, id string, update bool) (*Plan, error) {
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
	state, err := m.loadDesired()
	if err != nil {
		return nil, ErrUnsafeStore
	}
	m.stateMu.RLock()
	configured, available, document := m.registryURL != "", m.available, m.registry
	var registryCopy *registryDocument
	if document != nil {
		copyDocument := *document
		registryCopy = &copyDocument
	}
	m.stateMu.RUnlock()
	if !configured || !available || registryCopy == nil {
		return nil, ErrUnavailable
	}
	currentIndex := indexDesired(state.Plugins, id)
	if update && currentIndex < 0 {
		return nil, ErrNotInstalled
	}
	if !update && currentIndex >= 0 {
		return nil, ErrAlreadyInstalled
	}
	plugin, ok := findPlugin(registryCopy.Plugins, id)
	if !ok {
		return nil, ErrPluginNotFound
	}
	pluginType := plugin.Type
	if registryCopy.SchemaVersion == SchemaVersion {
		pluginType = TypeSource
	}
	if pluginType != TypeSource {
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
	if update && currentIndex >= 0 && state.Plugins[currentIndex].Version == release.Version {
		return nil, ErrNoUpdateAvailable
	}
	selected := DesiredPlugin{ID: plugin.ID, Name: plugin.Name, Version: release.Version, Channel: "stable", Digest: artifact.SHA256, Filename: artifact.Filename, Size: artifact.Size, SourceCommit: release.SourceCommit}
	if err := m.downloadAndVerify(ctx, plugin, release, artifact); err != nil {
		return nil, redactFailure(err)
	}
	proposed := desiredState{SchemaVersion: desiredSchemaVersion, Revision: state.Revision + 1, Plugins: cloneDesired(state.Plugins)}
	if proposed.Revision == 0 {
		return nil, ErrInstallFailed
	}
	if currentIndex >= 0 {
		proposed.Plugins[currentIndex] = selected
	} else {
		proposed.Plugins = append(proposed.Plugins, selected)
	}
	sort.Slice(proposed.Plugins, func(i, j int) bool { return proposed.Plugins[i].ID < proposed.Plugins[j].ID })
	setID, err := sourceSetID(proposed.Plugins)
	if err != nil {
		return nil, ErrInstallFailed
	}
	proposed.SourceSetID = setID
	if err := m.ensureArtifact(selected); err != nil {
		return nil, ErrUnsafeStore
	}
	sourceDir, err := m.ensureSourceSet(proposed.Plugins, setID)
	if err != nil {
		return nil, ErrInstallFailed
	}
	plan := &Plan{manager: m, previous: state, proposed: proposed, sourceDir: sourceDir, selection: selected}
	locked = false
	return plan, nil
}

func (m *Manager) PrepareUninstall(id string) (*Plan, error) {
	if m == nil || !validPluginID(id) {
		return nil, ErrPluginNotFound
	}
	m.gate <- struct{}{}
	state, err := m.loadDesired()
	if err != nil {
		m.release()
		return nil, ErrUnsafeStore
	}
	index := indexDesired(state.Plugins, id)
	if index < 0 {
		m.release()
		return nil, ErrNotInstalled
	}
	proposed := desiredState{SchemaVersion: desiredSchemaVersion, Revision: state.Revision + 1, Plugins: cloneDesired(state.Plugins)}
	if proposed.Revision == 0 {
		m.release()
		return nil, ErrInstallFailed
	}
	proposed.Plugins = append(proposed.Plugins[:index], proposed.Plugins[index+1:]...)
	setID, err := sourceSetID(proposed.Plugins)
	if err != nil {
		m.release()
		return nil, ErrInstallFailed
	}
	proposed.SourceSetID = setID
	sourceDir, err := m.ensureSourceSet(proposed.Plugins, setID)
	if err != nil {
		m.release()
		return nil, ErrInstallFailed
	}
	return &Plan{manager: m, previous: state, proposed: proposed, sourceDir: sourceDir}, nil
}

func indexDesired(plugins []DesiredPlugin, id string) int {
	for i, plugin := range plugins {
		if plugin.ID == id {
			return i
		}
	}
	return -1
}

func findPlugin(plugins []registryPlugin, id string) (registryPlugin, bool) {
	for _, plugin := range plugins {
		if plugin.ID == id {
			return plugin, true
		}
	}
	return registryPlugin{}, false
}

func (m *Manager) getBounded(ctx context.Context, rawURL string, max int64) ([]byte, error) {
	if !validHTTPSURL(rawURL, true) {
		return nil, ErrUnavailable
	}
	if err := validateNetworkURL(ctx, rawURL, m.injected); err != nil {
		return nil, ErrUnavailable
	}
	requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, ErrUnavailable
	}
	response, err := m.client.Do(request)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || (response.Request != nil && (response.Request.URL.Scheme != "https" || response.Request.URL.User != nil)) || response.ContentLength > max {
		return nil, ErrUnavailable
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, max+1))
	if err != nil || int64(len(data)) > max {
		return nil, ErrUnavailable
	}
	return data, nil
}

func (m *Manager) downloadAndVerify(ctx context.Context, plugin registryPlugin, release registryRelease, artifact registryArtifact) error {
	stageDir, err := os.MkdirTemp(filepath.Join(m.root, "staging"), "download-")
	if err != nil {
		return ErrInstallFailed
	}
	defer func() { _ = removeOwnedTree(stageDir) }()
	stagePath := filepath.Join(stageDir, artifact.Filename)
	requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	if err := validateNetworkURL(requestCtx, artifact.URL, m.injected); err != nil {
		return ErrDownloadFailed
	}
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, artifact.URL, nil)
	if err != nil {
		return ErrDownloadFailed
	}
	response, err := m.client.Do(request)
	if err != nil {
		return ErrDownloadFailed
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Request != nil && (response.Request.URL.Scheme != "https" || response.Request.URL.User != nil) {
		return ErrDownloadFailed
	}
	if response.ContentLength >= 0 && response.ContentLength != artifact.Size {
		return ErrVerificationFailed
	}
	if response.ContentLength > MaxArtifactBytes || artifact.Size > MaxArtifactBytes {
		return ErrVerificationFailed
	}
	output, err := os.OpenFile(stagePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return ErrInstallFailed
	}
	h := sha256.New()
	written, copyErr := io.CopyBuffer(io.MultiWriter(output, h), io.LimitReader(response.Body, artifact.Size+1), make([]byte, copyBufferSize))
	if copyErr == nil && written != artifact.Size {
		copyErr = ErrVerificationFailed
	}
	if copyErr == nil && hex.EncodeToString(h.Sum(nil)) != artifact.SHA256 {
		copyErr = ErrVerificationFailed
	}
	if copyErr == nil && ctx.Err() != nil {
		copyErr = ctx.Err()
	}
	if copyErr == nil {
		copyErr = output.Sync()
	}
	closeErr := output.Close()
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		if errors.Is(copyErr, ErrVerificationFailed) {
			return ErrVerificationFailed
		}
		if ctx.Err() != nil {
			return ErrDownloadFailed
		}
		return ErrDownloadFailed
	}
	// The executable bit is applied only after exact bytes and size pass the
	// Registry's digest pin. Protocol-specific black-box probing follows before
	// the verified artifact is published.
	if err := os.Chmod(stagePath, 0500); err != nil {
		return ErrInstallFailed
	}
	protocolVersion := release.ProtocolVersion
	if release.Protocol != nil {
		protocolVersion = release.Protocol.Version
	}
	var probeErr error
	if plugin.Type == TypeStorage {
		probeErr = probeStorageIdentity(ctx, stageDir, stagePath, plugin.ID, release.Version, protocolVersion)
	} else {
		probeErr = probeIdentity(ctx, stageDir, plugin.ID, release.Version, protocolVersion)
	}
	if probeErr != nil {
		return probeErr
	}
	if _, err := m.persistVerifiedArtifact(stagePath, artifact.SHA256, artifact.Size); err != nil {
		return err
	}
	return nil
}

func probeStorageIdentity(ctx context.Context, _ string, binary, id, version string, protocol int) error {
	probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	// The adapter is discovered from a nested staging directory, which can be
	// longer than the platform's Unix-domain socket pathname limit. Keep the
	// private IPC rendezvous under a short, separately-owned temp directory;
	// only the verified executable remains in the download staging area.
	probeDir, err := os.MkdirTemp("/tmp", "irpr-")
	if err != nil {
		return ErrIdentityMismatch
	}
	defer os.RemoveAll(probeDir)
	if err := os.Chmod(probeDir, 0700); err != nil {
		return ErrIdentityMismatch
	}
	socketPath := filepath.Join(probeDir, "p.sock")
	tokenPath := filepath.Join(probeDir, "t")
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return ErrIdentityMismatch
	}
	tokenText := []byte(hex.EncodeToString(token))
	if err := os.WriteFile(tokenPath, tokenText, 0600); err != nil {
		for i := range tokenText {
			tokenText[i] = 0
		}
		for i := range token {
			token[i] = 0
		}
		return ErrIdentityMismatch
	}
	client, err := storageproto.Start(probeCtx, storageproto.StartOptions{
		Binary: binary, SocketPath: socketPath, TokenFile: tokenPath, StartupTimeout: 5 * time.Second,
	})
	for i := range tokenText {
		tokenText[i] = 0
	}
	for i := range token {
		token[i] = 0
	}
	if err != nil {
		return ErrIdentityMismatch
	}
	defer func() { _ = client.Close() }()
	descriptor, err := client.Describe(probeCtx)
	if err != nil || storageproto.ValidateDescriptor(descriptor) != nil || descriptor.ID != id || descriptor.Version != version || descriptor.ProtocolVersion != protocol || descriptor.ProtocolVersion != storageproto.Version {
		return ErrIdentityMismatch
	}
	return nil
}

func probeIdentity(ctx context.Context, dir, id, version string, protocol int) error {
	probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	host, err := adapterhost.DiscoverDirs(probeCtx, []string{dir}, nil)
	if err != nil {
		return ErrIdentityMismatch
	}
	defer host.Close()
	adapters := host.List()
	if len(adapters) != 1 || adapters[0].Status.State != "ready" || adapters[0].Descriptor == nil {
		return ErrIdentityMismatch
	}
	descriptor := *adapters[0].Descriptor
	if descriptor.ID != id || descriptor.Version != version || descriptor.ProtocolVersion != protocol || descriptor.ProtocolVersion != adapterproto.Version {
		return ErrIdentityMismatch
	}
	if err := descriptor.Validate(); err != nil {
		return ErrIdentityMismatch
	}
	return nil
}

func (m *Manager) updateDesired(state desiredState) {
	m.stateMu.Lock()
	m.desired = state
	m.stateMu.Unlock()
}

func (m *Manager) readCurrentDesired() (desiredState, error) {
	return m.loadDesired()
}
