package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/buildinfo"
	"github.com/integrated-recorder/core/internal/derivative"
	"github.com/integrated-recorder/core/internal/integrity"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/network"
	"github.com/integrated-recorder/core/internal/pluginconfig"
	"github.com/integrated-recorder/core/internal/preview"
	"github.com/integrated-recorder/core/internal/server"
	"github.com/integrated-recorder/core/internal/storage"
	"github.com/integrated-recorder/core/internal/systemsettings"
	"github.com/integrated-recorder/core/internal/watch"
)

func main() {
	if err := run(); err != nil {
		log.Printf("archiver: %v", err)
		os.Exit(1)
	}
}

func run() error {
	addr := strings.TrimSpace(os.Getenv("ADDR"))
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = "./data"
	}
	adapterDirsValue := strings.TrimSpace(os.Getenv("ADAPTER_DIR"))
	if adapterDirsValue == "" {
		adapterDirsValue = "./adapters"
	}
	adapterDirs := filepath.SplitList(adapterDirsValue)

	store, err := storage.New(dataDir)
	if err != nil {
		return fmt.Errorf("initialize storage: %w", err)
	}
	settings, err := systemsettings.Open(dataDir)
	if err != nil {
		return fmt.Errorf("initialize system settings: %w", err)
	}
	startupSettings := settings.Current()
	if err := store.ConfigureIngestOptions(startupSettings.Storage.IngestOptions()); err != nil {
		return fmt.Errorf("configure storage ingest: %w", err)
	}
	configStore, secretStore, stateStore, err := pluginconfig.NewTypedFileStoresAndState(dataDir)
	if err != nil {
		return fmt.Errorf("initialize plugin settings: %w", err)
	}
	configs, err := pluginconfig.NewService(configStore, secretStore)
	if err != nil {
		return fmt.Errorf("initialize plugin settings: %w", err)
	}
	adapters, err := adapterhost.DiscoverDirs(context.Background(), adapterDirs, configs, stateStore)
	if err != nil {
		return fmt.Errorf("discover adapters: %w", err)
	}
	manager, err := acquire.NewManager(store, network.NewPublicHTTPClient(25*time.Second), adapters, nil)
	if err != nil {
		adapters.Close()
		return fmt.Errorf("load recordings: %w", err)
	}
	for _, issue := range store.RecoveryIssues() {
		log.Printf("storage recovery: recording=%s code=%s: %s", issue.ID, issue.Code, issue.Message)
	}

	products, err := management.Open(dataDir)
	if err != nil {
		_ = manager.Close(context.Background())
		adapters.Close()
		return fmt.Errorf("initialize management projections: %w", err)
	}
	for _, id := range products.DisabledAdapters() {
		if discovered, getErr := adapters.Get(id); getErr == nil && discovered.Descriptor != nil {
			if setErr := adapters.SetEnabled(id, false); setErr != nil {
				_ = manager.Close(context.Background())
				adapters.Close()
				return fmt.Errorf("apply adapter preference: %w", setErr)
			}
		}
	}
	integrityService, err := integrity.Open(dataDir, store, settings.IntegrityConcurrency())
	if err != nil {
		_ = manager.Close(context.Background())
		adapters.Close()
		return fmt.Errorf("initialize integrity verification: %w", err)
	}
	exportService, err := derivative.Open(dataDir, store, os.Getenv("FFMPEG_PATH"), 2)
	if err != nil {
		_ = integrityService.Close(context.Background())
		_ = manager.Close(context.Background())
		adapters.Close()
		return fmt.Errorf("initialize remux export: %w", err)
	}
	previewService, err := preview.OpenWithGet(dataDir, store, manager.List, manager.Get, os.Getenv("FFMPEG_PATH"))
	if err != nil {
		_ = exportService.Close(context.Background())
		_ = integrityService.Close(context.Background())
		_ = manager.Close(context.Background())
		adapters.Close()
		return fmt.Errorf("initialize preview projections: %w", err)
	}
	watchService, err := watch.New(dataDir, adapters, manager, watch.Options{
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
			return &watch.PreviewSummary{
				Mode: string(summary.Mode), State: string(summary.State), Available: summary.Available,
				FrameCount: summary.FrameCount, ImageArchiveOrdinal: summary.ImageArchiveOrdinal,
				LatestArchiveOrdinal: summary.LatestArchiveOrdinal, UpdatedAt: summary.UpdatedAt,
			}
		},
	})
	if err != nil {
		_ = previewService.Close(context.Background())
		_ = exportService.Close(context.Background())
		_ = integrityService.Close(context.Background())
		_ = manager.Close(context.Background())
		adapters.Close()
		return fmt.Errorf("initialize automatic recording watches: %w", err)
	}

	authDisabled := os.Getenv("AUTH_DISABLED") == "1"
	if authDisabled && !isLoopbackAddress(addr) {
		_ = previewService.Close(context.Background())
		_ = exportService.Close(context.Background())
		_ = integrityService.Close(context.Background())
		_ = manager.Close(context.Background())
		adapters.Close()
		return fmt.Errorf("AUTH_DISABLED is allowed only with a loopback ADDR")
	}
	var authService *authn.Service
	if !authDisabled {
		authService, err = authn.Open(dataDir)
		if err != nil {
			_ = previewService.Close(context.Background())
			_ = exportService.Close(context.Background())
			_ = integrityService.Close(context.Background())
			_ = manager.Close(context.Background())
			adapters.Close()
			return fmt.Errorf("initialize administrator authentication: %w", err)
		}
	}
	forceSecureCookies := os.Getenv("COOKIE_SECURE") == "1"
	startedAt := time.Now().UTC()

	build := buildinfo.Current()
	apiServer := server.NewWithOptions(manager, adapters, configs, server.Options{Management: products, Integrity: integrityService, Derivatives: exportService, Previews: previewService, Watches: watchService, Auth: authService, Settings: settings, InitialIntegrityConcurrency: startupSettings.Integrity.Concurrency, InitialStorageSettings: &startupSettings.Storage, ForceSecureCookies: forceSecureCookies, StartedAt: startedAt, BuildInfo: build})
	httpServer := &http.Server{Addr: addr, Handler: apiServer, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	shutdownCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	retentionDone := make(chan struct{})
	go func() {
		defer close(retentionDone)
		apiServer.RunRetention(shutdownCtx)
	}()
	previewDone := make(chan error, 1)
	go func() { previewDone <- previewService.Run(shutdownCtx) }()
	watchDone := make(chan error, 1)
	go func() { watchDone <- watchService.Run(shutdownCtx) }()
	serveErr := make(chan error, 1)
	go func() { serveErr <- httpServer.ListenAndServe() }()

	log.Printf("archiver listening on %s (data directory %s)", addr, dataDir)
	var runErr error
	select {
	case err = <-serveErr:
		if err != nil && err != http.ErrServerClosed {
			runErr = fmt.Errorf("HTTP server: %w", err)
		}
	case <-shutdownCtx.Done():
	}

	// Stop new HTTP work before stopping recording workers, then close adapter
	// processes only after workers have finished any in-flight adapter call.
	shutdownHTTP, cancelHTTP := context.WithTimeout(context.Background(), 10*time.Second)
	if err = httpServer.Shutdown(shutdownHTTP); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("HTTP shutdown: %w", err))
		_ = httpServer.Close()
	}
	cancelHTTP()
	// The retention runner shares the signal context and must stop before its
	// manager and projection dependencies are closed.
	stop()
	<-retentionDone
	if err = <-previewDone; err != nil && !errors.Is(err, context.Canceled) {
		runErr = errors.Join(runErr, fmt.Errorf("preview reconciliation shutdown: %w", err))
	}

	shutdownWorkers, cancelWorkers := context.WithTimeout(context.Background(), 15*time.Second)
	if err = watchService.Close(shutdownWorkers); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("watch scheduler shutdown: %w", err))
	}
	if err = <-watchDone; err != nil && !errors.Is(err, context.Canceled) {
		runErr = errors.Join(runErr, fmt.Errorf("watch scheduler: %w", err))
	}
	if err = previewService.Close(shutdownWorkers); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("preview shutdown: %w", err))
	}
	if err = exportService.Close(shutdownWorkers); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("export shutdown: %w", err))
	}
	if err = manager.Close(shutdownWorkers); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("recording shutdown: %w", err))
	}
	if err = integrityService.Close(shutdownWorkers); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("integrity shutdown: %w", err))
	}
	cancelWorkers()
	adapters.Close()
	if runErr != nil {
		return runErr
	}
	return nil
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
