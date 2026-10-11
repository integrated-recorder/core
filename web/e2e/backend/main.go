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
	"github.com/integrated-recorder/core/internal/derivative"
	"github.com/integrated-recorder/core/internal/integrity"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/pluginconfig"
	"github.com/integrated-recorder/core/internal/preview"
	"github.com/integrated-recorder/core/internal/runtimehost/httpapi"
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
	setupCode, err := authn.ReadSetupCode(dataDir)
	if err != nil {
		_ = watchService.Close(context.Background())
		_ = previewService.Close(context.Background())
		_ = exportService.Close(context.Background())
		_ = integrityService.Close(context.Background())
		_ = manager.Close(context.Background())
		adapters.Close()
		return fmt.Errorf("read isolated E2E setup credential: %w", err)
	}
	if err := auth.Bootstrap(setupCode, "browser-e2e-strong-password"); err != nil {
		_ = watchService.Close(context.Background())
		_ = previewService.Close(context.Background())
		_ = exportService.Close(context.Background())
		_ = integrityService.Close(context.Background())
		_ = manager.Close(context.Background())
		adapters.Close()
		return fmt.Errorf("seed isolated E2E administrator: %w", err)
	}
	api := server.NewWithOptions(manager, adapters, configs, server.Options{
		Management: products, Integrity: integrityService, Derivatives: exportService,
		Previews: previewService, Watches: watchService,
		Auth: auth, Settings: settings, InitialIntegrityConcurrency: startupSettings.Integrity.Concurrency, InitialStorageSettings: &startupSettings.Storage,
		StartedAt: time.Now().UTC(), Version: "browser-e2e", Commit: "test-fixture",
	})

	addr := strings.TrimSpace(os.Getenv("ADDR"))
	if addr == "" {
		addr = "127.0.0.1:4173"
	}
	web := &http.Server{Addr: addr, Handler: legacyWorkflowReadyHostProjection(api, auth), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
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
				"claim_required": false, "recovery_required": false,
				"auth_disabled": false, "version": "browser-e2e",
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
	principal := authn.Principal{UserID: session.UserID, Login: session.Login, Role: session.Role}
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
