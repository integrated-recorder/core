package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/authority"
	"github.com/integrated-recorder/core/internal/derivative"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/integrity"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/pluginconfig"
	"github.com/integrated-recorder/core/internal/preview"
	"github.com/integrated-recorder/core/internal/runtimehost/httpapi"
	"github.com/integrated-recorder/core/internal/runtimehost/tlsserver"
	"github.com/integrated-recorder/core/internal/server"
	"github.com/integrated-recorder/core/internal/storage"
	"github.com/integrated-recorder/core/internal/storageproto"
	"github.com/integrated-recorder/core/internal/systemsettings"
	"github.com/integrated-recorder/core/internal/watch"
)

const segmentPayload = "integrated-recorder-browser-e2e-source-segment"

type sourceFixture struct {
	mu                sync.RWMutex
	playlist          []byte
	segments          map[string][]byte
	streamTitle       string
	streamDescription string
	online            bool
	endlist           bool
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--identity-peer" {
		if err := runIdentityPeerProcess(); err != nil {
			log.Printf("browser e2e identity peer: %v", err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		log.Printf("browser e2e backend: %v", err)
		os.Exit(1)
	}
}

func run() error {
	root := repositoryRoot()
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		return errors.New("DATA_DIR is required")
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return err
	}
	adapterDir := filepath.Join(dataDir, "adapters")
	if err := os.MkdirAll(adapterDir, 0o700); err != nil {
		return err
	}
	for _, build := range []struct {
		name string
		pkg  string
	}{
		{name: "integrated-recorder-adapter-hls", pkg: "./cmd/adapters/hls"},
		{name: "integrated-recorder-adapter-workflow-fixture", pkg: "./web/e2e/fixture_adapter"},
		{name: "integrated-recorder-adapter-metadata-fixture", pkg: "./web/e2e/metadata_fixture_adapter"},
		{name: "integrated-recorder-adapter-timed-event-fixture", pkg: "./web/e2e/timed_event_fixture_adapter"},
	} {
		command := exec.Command("go", "build", "-trimpath", "-o", filepath.Join(adapterDir, build.name), build.pkg)
		command.Dir = root
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("build %s: %w\n%s", build.name, err, output)
		}
	}

	fixture, ffmpegPath := buildSourceFixture(dataDir)
	source := http.Server{Handler: http.HandlerFunc(fixture.serveHTTP)}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	go func() { _ = source.Serve(listener) }()
	defer source.Close()
	sourceURL := "http://" + listener.Addr().String()
	if err = os.WriteFile(filepath.Join(dataDir, "e2e-source-url"), []byte(sourceURL), 0o600); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(dataDir, "e2e-preview-available"), []byte(fmt.Sprint(ffmpegPath != "")), 0o600); err != nil {
		return err
	}
	store, err := storage.New(dataDir)
	if err != nil {
		return err
	}
	settings, err := systemsettings.Open(dataDir)
	if err != nil {
		return err
	}
	startupSettings := settings.Current()
	if err = store.ConfigureIngestOptions(startupSettings.Storage.IngestOptions()); err != nil {
		return err
	}
	configStore, secretStore, stateStore, err := pluginconfig.NewTypedFileStoresAndState(dataDir)
	if err != nil {
		return err
	}
	configs, err := pluginconfig.NewService(configStore, secretStore)
	if err != nil {
		return err
	}
	adapters, err := adapterhost.DiscoverDirs(context.Background(), []string{adapterDir}, configs, stateStore)
	if err != nil {
		return err
	}
	validateFixtureOrigin := func(_ context.Context, raw string) error {
		u, parseErr := url.Parse(raw)
		if parseErr != nil || u.User != nil || u.Scheme != "http" || u.Host != listener.Addr().String() {
			return errors.New("fixture source origin is not allowed")
		}
		return nil
	}
	manager, err := acquire.NewManager(store, &http.Client{Timeout: 25 * time.Second}, adapters, validateFixtureOrigin)
	if err != nil {
		adapters.Close()
		return err
	}
	products, err := management.Open(dataDir)
	if err != nil {
		_ = manager.Close(context.Background())
		adapters.Close()
		return err
	}
	integrityService, err := integrity.Open(dataDir, store, settings.IntegrityConcurrency())
	if err != nil {
		_ = manager.Close(context.Background())
		adapters.Close()
		return err
	}
	// The export service requires an absolute, resolvable path. A directory is
	// intentionally not an executable, so the test reports FFmpeg unavailable
	// deterministically even on developers' machines that have FFmpeg installed.
	disabledFFmpegPath := filepath.Join(dataDir, "ffmpeg-disabled")
	if err := os.MkdirAll(disabledFFmpegPath, 0o700); err != nil {
		_ = manager.Close(context.Background())
		adapters.Close()
		return err
	}
	exportService, err := derivative.Open(dataDir, store, disabledFFmpegPath, 1)
	if err != nil {
		_ = integrityService.Close(context.Background())
		_ = manager.Close(context.Background())
		adapters.Close()
		return err
	}
	previewService, err := preview.OpenWithGet(dataDir, store, manager.List, manager.Get, ffmpegPath)
	if err != nil {
		_ = exportService.Close(context.Background())
		_ = integrityService.Close(context.Background())
		_ = manager.Close(context.Background())
		adapters.Close()
		return err
	}
	watchService, err := watch.New(dataDir, adapters, manager, watch.Options{})
	if err != nil {
		_ = previewService.Close(context.Background())
		_ = exportService.Close(context.Background())
		_ = integrityService.Close(context.Background())
		_ = manager.Close(context.Background())
		adapters.Close()
		return err
	}
	auth, err := authn.Open(dataDir)
	if err != nil {
		_ = watchService.Close(context.Background())
		_ = previewService.Close(context.Background())
		_ = exportService.Close(context.Background())
		_ = integrityService.Close(context.Background())
		_ = manager.Close(context.Background())
		adapters.Close()
		return err
	}
	if err := auth.Bootstrap("owner", "browser-e2e-strong-password"); err != nil {
		_ = watchService.Close(context.Background())
		_ = previewService.Close(context.Background())
		_ = exportService.Close(context.Background())
		_ = integrityService.Close(context.Background())
		_ = manager.Close(context.Background())
		adapters.Close()
		return fmt.Errorf("seed isolated E2E administrator: %w", err)
	}
	identityStore, err := globalidentity.Open(dataDir)
	if err != nil {
		_ = watchService.Close(context.Background())
		_ = previewService.Close(context.Background())
		_ = exportService.Close(context.Background())
		_ = integrityService.Close(context.Background())
		_ = manager.Close(context.Background())
		adapters.Close()
		return fmt.Errorf("open isolated global identity store: %w", err)
	}
	identityRegistry, err := globalidentity.OpenHistoryRegistry(dataDir)
	if err != nil {
		_ = watchService.Close(context.Background())
		_ = previewService.Close(context.Background())
		_ = exportService.Close(context.Background())
		_ = integrityService.Close(context.Background())
		_ = manager.Close(context.Background())
		adapters.Close()
		return fmt.Errorf("open isolated public identity history registry: %w", err)
	}
	nodeIdentityStore, err := nodeidentity.Open(dataDir)
	if err != nil {
		_ = watchService.Close(context.Background())
		_ = previewService.Close(context.Background())
		_ = exportService.Close(context.Background())
		_ = integrityService.Close(context.Background())
		_ = manager.Close(context.Background())
		adapters.Close()
		return fmt.Errorf("open isolated Node Key store: %w", err)
	}
	ownerAuthorityStore, err := authority.Open(dataDir)
	if err != nil {
		return fmt.Errorf("open isolated OwnerSubject authority store: %w", err)
	}
	var playbackPeer *e2ePlaybackPeer
	if os.Getenv("IR_E2E_SECOND_NODE") == "1" {
		playbackPeer = newE2EPlaybackPeer(identityStore)
	}
	api := server.NewWithOptions(manager, adapters, configs, server.Options{
		Management: products, Integrity: integrityService, Derivatives: exportService,
		Previews: previewService, Watches: watchService,
		Auth: auth, GlobalIdentity: identityStore, GlobalIdentityRegistry: identityRegistry, OwnerAuthority: ownerAuthorityStore, NodeIdentity: nodeIdentityStore, Settings: settings, InitialIntegrityConcurrency: startupSettings.Integrity.Concurrency, InitialStorageSettings: &startupSettings.Storage,
		PeerSessions: playbackPeer,
		StartedAt:    time.Now().UTC(), Version: "browser-e2e", Commit: "test-fixture",
	})
	httpsProbe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("select HTTPS browser fixture port: %w", err)
	}
	httpsPort := httpsProbe.Addr().(*net.TCPAddr).Port
	_ = httpsProbe.Close()
	httpsAddress := net.JoinHostPort("127.0.0.1", strconv.Itoa(httpsPort))
	httpsManager, err := tlsserver.Open(dataDir, httpsAddress, httpsPort)
	if err != nil {
		return fmt.Errorf("initialize HTTPS browser fixture: %w", err)
	}
	if err := httpsManager.SetHTTPPublicPort(4173); err != nil {
		_ = httpsManager.Close(context.Background())
		return fmt.Errorf("configure HTTP browser fixture port: %w", err)
	}
	publicHandler, err := httpapi.WithHTTPS(auth, false, httpsManager, legacyWorkflowReadyHostProjection(api, auth))
	if err != nil {
		_ = httpsManager.Close(context.Background())
		return err
	}
	publicHandler = httpsManager.WrapHTTP(publicHandler)
	if err := httpsManager.SetHandler(publicHandler); err != nil {
		_ = httpsManager.Close(context.Background())
		return fmt.Errorf("start HTTPS browser fixture: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = httpsManager.Close(closeCtx)
	}()
	initialHTTPSURL := "https://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(httpsPort))
	if err := os.WriteFile(filepath.Join(dataDir, "e2e-https-url"), []byte(initialHTTPSURL), 0o600); err != nil {
		return fmt.Errorf("record HTTPS browser fixture URL: %w", err)
	}
	var identityPeer *identityPeerManager
	if os.Getenv("IR_E2E_SECOND_NODE") == "1" {
		if err := os.WriteFile(filepath.Join(dataDir, "e2e-node-a-pid"), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			return fmt.Errorf("record isolated first-node process identity: %w", err)
		}
		identityPeer, err = startIdentityPeerManager(dataDir)
		if err != nil {
			return fmt.Errorf("start isolated second identity process: %w", err)
		}
		defer identityPeer.Close()
	}

	addr := strings.TrimSpace(os.Getenv("ADDR"))
	if addr == "" {
		addr = "127.0.0.1:4173"
	}
	web := &http.Server{Addr: addr, Handler: identityPeerTestControl(publicHandler, identityPeer, playbackPeer), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- web.ListenAndServe() }()
	log.Printf("browser e2e API listening on %s", addr)
	previewCtx, cancelPreview := context.WithCancel(context.Background())
	previewDone := make(chan error, 1)
	go func() { previewDone <- previewService.Run(previewCtx) }()
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	watchDone := make(chan error, 1)
	go func() { watchDone <- watchService.Run(watchCtx) }()

	shutdown, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case err = <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			cancelPreview()
			cancelWatch()
			<-watchDone
			<-previewDone
			_ = watchService.Close(context.Background())
			_ = closeServices(manager, integrityService, exportService, previewService, adapters)
			return err
		}
	case <-shutdown.Done():
	}
	stop()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = web.Shutdown(ctx)
	cancelWatch()
	<-watchDone
	cancelPreview()
	<-previewDone
	if err = previewService.Close(ctx); err != nil {
		return err
	}
	if err = watchService.Close(ctx); err != nil {
		return err
	}
	if err = manager.Close(ctx); err != nil {
		return err
	}
	if err = exportService.Close(ctx); err != nil {
		return err
	}
	if err = integrityService.Close(ctx); err != nil {
		return err
	}
	adapters.Close()
	return nil
}

// startIdentityPeer starts a separate OS process with its own durable state and
// Control Plane listener. The focused identity Playwright project enables it
// only with an isolated temporary data root.
type identityPeerProcess struct {
	cmd     *exec.Cmd
	done    chan struct{}
	waitErr error
}

type identityPeerManager struct {
	mu      sync.Mutex
	dataDir string
	process *identityPeerProcess
}

func startIdentityPeerManager(dataDir string) (*identityPeerManager, error) {
	_, process, err := startIdentityPeer(dataDir)
	if err != nil {
		return nil, err
	}
	return &identityPeerManager{dataDir: dataDir, process: process}, nil
}

func (m *identityPeerManager) Restart() error {
	if m == nil {
		return errors.New("identity peer manager is unavailable")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stopIdentityPeerProcess(m.process)
	m.process = nil
	_, process, err := startIdentityPeer(m.dataDir)
	if err != nil {
		return err
	}
	m.process = process
	return nil
}

func (m *identityPeerManager) Close() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stopIdentityPeerProcess(m.process)
	m.process = nil
}

func identityPeerTestControl(next http.Handler, peer *identityPeerManager, playback *e2ePlaybackPeer) http.Handler {
	if peer == nil && playback == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/__e2e/peer-playback/connect" && playback != nil {
			if r.Method != http.MethodPost {
				w.Header().Set("Allow", http.MethodPost)
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			var request struct {
				NodeID string `json:"node_id"`
			}
			if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&request); err != nil || !validE2EPeerNodeID(request.NodeID) {
				http.Error(w, "invalid isolated peer fixture", http.StatusBadRequest)
				return
			}
			playback.ConnectNode(request.NodeID)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.URL.Path != "/__e2e/identity-peer/restart" || peer == nil {
			next.ServeHTTP(w, r)
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := peer.Restart(); err != nil {
			http.Error(w, "identity peer restart failed", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func startIdentityPeer(dataDir string) (string, *identityPeerProcess, error) {
	peerData := filepath.Join(dataDir, "identity-peer-b")
	if err := os.MkdirAll(peerData, 0o700); err != nil {
		return "", nil, err
	}
	urlFile := filepath.Join(dataDir, "e2e-node-b-url")
	if err := os.Remove(urlFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", nil, err
	}
	pidFile := filepath.Join(dataDir, "e2e-node-b-pid")
	if err := os.Remove(pidFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", nil, err
	}
	executable, err := os.Executable()
	if err != nil {
		return "", nil, err
	}
	peer := exec.Command(executable, "--identity-peer")
	peer.Env = replaceEnvironment(os.Environ(), map[string]string{
		"IR_E2E_IDENTITY_PEER_DATA_DIR": peerData,
		"IR_E2E_NODE_B_URL_FILE":        urlFile,
	})
	peer.Stdout = os.Stdout
	peer.Stderr = os.Stderr
	if err := peer.Start(); err != nil {
		return "", nil, err
	}
	process := &identityPeerProcess{cmd: peer, done: make(chan struct{})}
	go func() {
		process.waitErr = peer.Wait()
		close(process.done)
	}()
	ready := false
	defer func() {
		if !ready {
			stopIdentityPeerProcess(process)
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	client := &http.Client{Timeout: time.Second}
	for time.Now().Before(deadline) {
		select {
		case <-process.done:
			if process.waitErr != nil {
				return "", nil, fmt.Errorf("identity peer exited during startup: %w", process.waitErr)
			}
			return "", nil, errors.New("identity peer exited cleanly before becoming ready")
		default:
		}
		if data, readErr := os.ReadFile(urlFile); readErr == nil {
			url := strings.TrimSpace(string(data))
			request, requestErr := http.NewRequest(http.MethodGet, url+"/login", nil)
			if requestErr != nil {
				return "", nil, fmt.Errorf("create identity peer readiness request: %w", requestErr)
			}
			request.Header.Set("Accept", "text/html")
			response, requestErr := client.Do(request)
			if requestErr == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					ready = true
					return url, process, nil
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", nil, errors.New("identity peer did not become ready within 10 seconds")
}

func runIdentityPeerProcess() error {
	dataDir := strings.TrimSpace(os.Getenv("IR_E2E_IDENTITY_PEER_DATA_DIR"))
	urlFile := strings.TrimSpace(os.Getenv("IR_E2E_NODE_B_URL_FILE"))
	if dataDir == "" || urlFile == "" {
		return errors.New("isolated identity peer configuration is missing")
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return err
	}
	auth, err := authn.Open(dataDir)
	if err != nil {
		return err
	}
	if auth.NeedsBootstrap() {
		if err := auth.Bootstrap("owner", "browser-e2e-strong-password"); err != nil {
			return err
		}
	}
	identity, err := globalidentity.Open(dataDir)
	if err != nil {
		return err
	}
	node, err := nodeidentity.Open(dataDir)
	if err != nil {
		return err
	}
	ownerAuthority, err := authority.Open(dataDir)
	if err != nil {
		return err
	}
	products, err := management.Open(dataDir)
	if err != nil {
		return err
	}
	handler := legacyWorkflowReadyHostProjection(server.NewWithOptions(nil, nil, nil, server.Options{Auth: auth, GlobalIdentity: identity, OwnerAuthority: ownerAuthority, NodeIdentity: node, Management: products}), auth)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	address := listener.Addr().(*net.TCPAddr)
	url := "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(address.Port))
	pidFile := filepath.Join(filepath.Dir(urlFile), "e2e-node-b-pid")
	if err := os.WriteFile(urlFile, []byte(url), 0o600); err != nil {
		_ = listener.Close()
		return err
	}
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		_ = listener.Close()
		return err
	}
	web := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- web.Serve(listener) }()
	shutdown, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-shutdown.Done():
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return web.Shutdown(ctx)
	}
	return nil
}

func stopIdentityPeerProcess(peer *identityPeerProcess) {
	if peer == nil || peer.cmd == nil || peer.cmd.Process == nil {
		return
	}
	select {
	case <-peer.done:
		return
	default:
	}
	_ = peer.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-peer.done:
	case <-time.After(5 * time.Second):
		_ = peer.cmd.Process.Kill()
		<-peer.done
	}
}

func replaceEnvironment(environment []string, values map[string]string) []string {
	output := make([]string, 0, len(environment)+len(values))
	for _, entry := range environment {
		key, _, found := strings.Cut(entry, "=")
		if _, replace := values[key]; !replace || !found {
			output = append(output, entry)
		}
	}
	for key, value := range values {
		output = append(output, key+"="+value)
	}
	return output
}

// legacyWorkflowReadyHostProjection exists only for the legacy application-
// workflow E2E server, which runs the Control Plane in process without a
// Runtime Host. The separate setup E2E exercises the real Host-owned setup API.
// Keep this projection narrowly scoped so every other request reaches the real
// application server unchanged.
func legacyWorkflowReadyHostProjection(next http.Handler, auth *authn.Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/api/setup/status" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"state": "ready", "administrator_configured": true,
				"recovery_required": false,
				"auth_disabled":     false, "version": "browser-e2e",
				"release_channel": "development",
			})
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/api/runtime/plugins" {
			serveFixtureRuntimeRead(w, r, auth, authn.PermissionPluginManage, httpapi.PluginStatus{
				State: "not_configured", Plugins: []httpapi.PluginStatusItem{},
			})
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/api/runtime/storage/provider" {
			// This legacy in-process fixture uses Core's local archive backend,
			// not Runtime Host. Project it as the Host's bundled local provider so
			// the current UI sees the same ready local-storage contract.
			serveFixtureRuntimeRead(w, r, auth, authn.PermissionStorageManage, httpapi.StorageProviderStatus{
				Primary: httpapi.PrimaryStorageStatus{Kind: "plugin", ProviderID: "local", Version: "browser-e2e", State: "ready"},
				Providers: []httpapi.StorageProviderSummary{{
					ID: "local", Name: "Local Storage", Version: "browser-e2e",
					Distribution: "bundled", ConfigurationManaged: true, Uninstallable: false,
					Configured: true, Active: true, Health: "ready",
					ConfigurationSchema: storageproto.Schema{Fields: []storageproto.Field{}},
				}},
			})
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/api/runtime/storage/instances" {
			// The in-process E2E server has no Runtime Host storage catalog.
			// Return its truthful empty instance inventory, not an API failure.
			serveFixtureRuntimeRead(w, r, auth, authn.PermissionStorageManage, []httpapi.StorageInstanceSummary{})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func serveFixtureRuntimeRead(w http.ResponseWriter, r *http.Request, auth *authn.Service, permission authn.Permission, value any) {
	if auth == nil {
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return
	}
	session, err := auth.Authenticate(authn.SessionToken(r))
	if err != nil {
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return
	}
	principal := authn.Principal{UserID: session.UserID, Login: session.Login, Roles: session.Roles, Permissions: session.Permissions, Role: session.Role}
	if !authn.HasPermission(principal, permission) {
		http.Error(w, "permission denied", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		return
	}
}

func closeServices(manager *acquire.Manager, integrityService *integrity.Service, exportService *derivative.Service, previewService *preview.Service, adapters *adapterhost.Host) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	defer adapters.Close()
	return errors.Join(manager.Close(ctx), exportService.Close(ctx), integrityService.Close(ctx), previewService.Close(ctx))
}

func repositoryRoot() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../../.."))
}

func buildSourceFixture(dataDir string) (*sourceFixture, string) {
	fallback := &sourceFixture{playlist: []byte("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:7\n#EXTINF:1.5,browser fixture\nsegment-7.ts\n"), segments: map[string][]byte{"segment-7.ts": []byte(segmentPayload)}, streamTitle: "Fixture broadcast title", streamDescription: "Initial fixture description"}
	_ = os.WriteFile(filepath.Join(dataDir, "e2e-source-segment"), fallback.segments["segment-7.ts"], 0600)
	ffmpegPath, err := exec.LookPath("ffmpeg")
	if err != nil {
		return fallback, ""
	}
	outputDir := filepath.Join(dataDir, "e2e-hls-source")
	if err := os.MkdirAll(outputDir, 0700); err != nil {
		return fallback, ""
	}
	manifestPath := filepath.Join(outputDir, "stream.m3u8")
	segmentPattern := filepath.Join(outputDir, "source-%03d.ts")
	base := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=25", "-t", "10"}
	variants := [][]string{
		{"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-g", "50", "-sc_threshold", "0", "-an"},
		{"-c:v", "mpeg2video", "-pix_fmt", "yuv420p", "-g", "12", "-an"},
	}
	generated := false
	for _, codec := range variants {
		_ = os.RemoveAll(outputDir)
		if err := os.MkdirAll(outputDir, 0700); err != nil {
			return fallback, ""
		}
		arguments := append(append([]string{}, base...), codec...)
		arguments = append(arguments, "-f", "hls", "-hls_time", "2", "-hls_list_size", "0", "-hls_segment_filename", segmentPattern, manifestPath)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		command := exec.CommandContext(ctx, ffmpegPath, arguments...)
		_, runErr := command.CombinedOutput()
		cancel()
		if runErr == nil {
			generated = true
			break
		}
	}
	if !generated {
		_ = os.RemoveAll(outputDir)
		return fallback, ""
	}
	playlist, err := os.ReadFile(manifestPath)
	if err != nil {
		return fallback, ""
	}
	scanner := bufio.NewScanner(bytes.NewReader(playlist))
	var published strings.Builder
	segments := map[string][]byte{}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || line == "#EXT-X-ENDLIST" {
			continue
		}
		if strings.HasPrefix(line, "#EXT-X-MEDIA-SEQUENCE:") {
			line = "#EXT-X-MEDIA-SEQUENCE:7"
		}
		if !strings.HasPrefix(line, "#") {
			name := filepath.Base(line)
			payload, readErr := os.ReadFile(filepath.Join(outputDir, name))
			if readErr != nil || len(payload) == 0 {
				return fallback, ""
			}
			segments[name] = payload
		}
		published.WriteString(line)
		published.WriteByte('\n')
	}
	if scanner.Err() != nil || len(segments) < 2 {
		return fallback, ""
	}
	fallback.playlist, fallback.segments = []byte(published.String()), segments
	for _, name := range playlistSegmentNames(playlist) {
		if payload := segments[name]; len(payload) > 0 {
			if err := os.WriteFile(filepath.Join(dataDir, "e2e-source-segment"), payload, 0600); err != nil {
				return fallback, ""
			}
			break
		}
	}
	return fallback, ffmpegPath
}

func playlistSegmentNames(playlist []byte) []string {
	scanner := bufio.NewScanner(bytes.NewReader(playlist))
	var names []string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			names = append(names, filepath.Base(line))
		}
	}
	return names
}

func (fixture *sourceFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/status" {
		fixture.mu.RLock()
		online := fixture.online
		fixture.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"online": online})
		return
	}
	if r.URL.Path == "/e2e/set-online" {
		online, onlineErr := strconv.ParseBool(r.URL.Query().Get("value"))
		endlist, endlistErr := strconv.ParseBool(r.URL.Query().Get("endlist"))
		if onlineErr != nil || endlistErr != nil {
			http.Error(w, "invalid fixture state", http.StatusBadRequest)
			return
		}
		fixture.mu.Lock()
		fixture.online, fixture.endlist = online, endlist
		fixture.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.URL.Path == "/e2e/metadata" {
		fixture.mu.RLock()
		title, description := fixture.streamTitle, fixture.streamDescription
		fixture.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"title": title, "description": description})
		return
	}
	if r.URL.Path == "/e2e/set-metadata" {
		title := r.URL.Query().Get("title")
		description := r.URL.Query().Get("description")
		if len(title) > 4096 || len(description) > 64<<10 {
			http.Error(w, "metadata is too long", http.StatusBadRequest)
			return
		}
		fixture.mu.Lock()
		fixture.streamTitle, fixture.streamDescription = title, description
		fixture.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.URL.Path == "/hls/stream.m3u8" {
		fixture.mu.RLock()
		playlist := append([]byte(nil), fixture.playlist...)
		endlist := fixture.endlist
		fixture.mu.RUnlock()
		if endlist && !bytes.Contains(playlist, []byte("#EXT-X-ENDLIST")) {
			playlist = append(playlist, []byte("#EXT-X-ENDLIST\n")...)
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		_, _ = w.Write(playlist)
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/hls/") {
		http.NotFound(w, r)
		return
	}
	name := filepath.Base(r.URL.Path)
	fixture.mu.RLock()
	payload, ok := fixture.segments[name]
	payload = append([]byte(nil), payload...)
	fixture.mu.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "video/mp2t")
	_, _ = w.Write(payload)
}
