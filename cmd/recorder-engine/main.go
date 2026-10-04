// recorder-engine runs one generation's long-lived acquisition manager. It
// exposes only a private local IPC socket; the Runtime Host controls process
// lifetime and application HTTP traffic remains in the Control Plane.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/pluginconfig"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/runtimehost/resources"
	"github.com/integrated-recorder/core/internal/runtimeipc"
	"github.com/integrated-recorder/core/internal/storage"
	"github.com/integrated-recorder/core/internal/storageprocess"
	"github.com/integrated-recorder/core/internal/systemsettings"
)

func main() {
	if err := run(); err != nil {
		log.Printf("recorder-engine: %v", err)
		os.Exit(1)
	}
}

func run() error {
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = "./data"
	}
	socketPath := strings.TrimSpace(os.Getenv("ENGINE_SOCKET_PATH"))
	generationID := strings.TrimSpace(os.Getenv("ENGINE_GENERATION_ID"))
	recoveryMode := strings.TrimSpace(os.Getenv("ENGINE_RECOVERY_MODE"))
	tokenPath := strings.TrimSpace(os.Getenv("ENGINE_IPC_TOKEN_FILE"))
	if socketPath == "" || generationID == "" || tokenPath == "" {
		return errors.New("ENGINE_SOCKET_PATH, ENGINE_GENERATION_ID, and ENGINE_IPC_TOKEN_FILE are required")
	}
	var mode acquire.StartupMode
	switch recoveryMode {
	case "recover":
		mode = acquire.RecoverExisting
	case "fresh":
		mode = acquire.FreshGeneration
	default:
		return errors.New("ENGINE_RECOVERY_MODE must be explicitly set to recover or fresh")
	}
	token, err := loadToken(tokenPath)
	if err != nil {
		return err
	}

	store, providerRuntime, err := storageprocess.OpenStore(context.Background(), dataDir,
		strings.TrimSpace(os.Getenv("STORAGE_PROVIDER_CATALOG_ROOT")), strings.TrimSpace(os.Getenv("STORAGE_PROVIDER_SET_ID")))
	if err != nil {
		return errors.New("initialize generation-pinned archive storage")
	}
	if providerRuntime != nil {
		defer func() { _ = providerRuntime.Close() }()
	}
	settings, err := systemsettings.Open(dataDir)
	if err != nil {
		return fmt.Errorf("initialize storage settings: %w", err)
	}
	if err := store.ConfigureIngestOptions(settings.Current().Storage.IngestOptions()); err != nil {
		return fmt.Errorf("configure storage ingest: %w", err)
	}
	runtimeClient, managedRuntime, err := configureRuntimeResourceClient(store, os.Getenv)
	if err != nil {
		return fmt.Errorf("configure runtime resource coordinator: %w", err)
	}
	var ownerStore *recordingowner.Store
	if managedRuntime {
		ownerStore, err = recordingowner.Open(dataDir)
		if err != nil {
			return fmt.Errorf("configure canonical recording owner fence: %w", err)
		}
	}
	configStore, secretStore, stateStore, err := pluginconfig.NewTypedFileStoresAndState(dataDir)
	if err != nil {
		return fmt.Errorf("initialize adapter configuration: %w", err)
	}
	configs, err := pluginconfig.NewService(configStore, secretStore)
	if err != nil {
		return fmt.Errorf("initialize adapter configuration: %w", err)
	}
	adapterDirsValue := strings.TrimSpace(os.Getenv("ADAPTER_DIR"))
	if adapterDirsValue == "" {
		adapterDirsValue = "./adapters"
	}
	adapters, err := adapterhost.DiscoverDirs(context.Background(), filepath.SplitList(adapterDirsValue), configs, stateStore)
	if err != nil {
		return fmt.Errorf("discover adapters: %w", err)
	}
	client, validateSource := newAcquisitionClient(25 * time.Second)
	var manager *acquire.Manager
	if managedRuntime && mode == acquire.RecoverExisting {
		manager, err = acquire.NewManagerWithFencedRecovery(store, client, adapters, validateSource, ownerStore, ownerStore)
	} else if managedRuntime {
		manager, err = acquire.NewManagerWithMode(store, client, adapters, validateSource, mode)
		if err == nil {
			err = manager.ConfigureCanonicalCommitFence(ownerStore)
		}
	} else if mode == acquire.RecoverExisting {
		manager, err = acquire.NewManager(store, client, adapters, validateSource)
	} else {
		manager, err = acquire.NewManagerWithMode(store, client, adapters, validateSource, mode)
	}
	if err != nil {
		if manager != nil {
			_ = manager.Close(context.Background())
		}
		adapters.Close()
		return fmt.Errorf("initialize recorder manager: %w", err)
	}
	instanceID, err := engineInstanceID(os.Getenv)
	if err != nil {
		_ = manager.Close(context.Background())
		adapters.Close()
		return err
	}
	engine, err := recorderengine.New(manager, adapters, generationID, instanceID)
	if err != nil {
		_ = manager.Close(context.Background())
		adapters.Close()
		return err
	}
	if managedRuntime {
		if err := engine.ConfigureRecordingOwnerClient(runtimeClient); err != nil {
			_ = engine.Close(context.Background())
			return fmt.Errorf("configure Host recording ownership client: %w", err)
		}
	}
	server, err := runtimeipc.NewServer(socketPath, generationID, token, engine)
	if err != nil {
		_ = engine.Close(context.Background())
		return err
	}
	shutdownCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(shutdownCtx) }()
	select {
	case err = <-serveDone:
		if err != nil {
			stop()
		}
	case <-shutdownCtx.Done():
	}
	stop()
	closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	closeErr := engine.Close(closeCtx)
	if err != nil {
		return errors.Join(err, closeErr)
	}
	return closeErr
}

func loadToken(path string) ([]byte, error) {
	return loadPrivateToken(path, "ENGINE_IPC_TOKEN_FILE")
}

func loadPrivateToken(path, variableName string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("%s must be a private regular file", variableName)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%s could not be read", variableName)
	}
	if len(data) != 32 {
		return nil, fmt.Errorf("%s must contain exactly 32 bytes", variableName)
	}
	return data, nil
}

func configureRuntimeResources(store *storage.Store, getenv func(string) string) error {
	_, _, err := configureRuntimeResourceClient(store, getenv)
	return err
}

func configureRuntimeResourceClient(store *storage.Store, getenv func(string) string) (*resources.RuntimeClient, bool, error) {
	if store == nil || getenv == nil {
		return nil, false, errors.New("runtime resource configuration is unavailable")
	}
	socketPath := strings.TrimSpace(getenv("RUNTIME_RESOURCE_SOCKET_PATH"))
	tokenPath := strings.TrimSpace(getenv("RUNTIME_RESOURCE_TOKEN_FILE"))
	ownerID := strings.TrimSpace(getenv("RUNTIME_RESOURCE_OWNER"))
	present := 0
	for _, value := range []string{socketPath, tokenPath, ownerID} {
		if value != "" {
			present++
		}
	}
	if present == 0 {
		// Direct development/compatibility invocation retains the historical
		// process-local limits. Runtime Host always supplies all three values.
		return nil, false, nil
	}
	if present != 3 {
		return nil, false, errors.New("RUNTIME_RESOURCE_SOCKET_PATH, RUNTIME_RESOURCE_TOKEN_FILE, and RUNTIME_RESOURCE_OWNER must be set together")
	}
	token, err := loadPrivateToken(tokenPath, "RUNTIME_RESOURCE_TOKEN_FILE")
	if err != nil {
		return nil, false, err
	}
	client, err := resources.NewRuntimeClient(socketPath, token, ownerID)
	if err != nil {
		return nil, false, err
	}
	if err := store.ConfigureRuntimeIngestCoordinator(client); err != nil {
		return nil, false, err
	}
	if err := store.ConfigureRuntimeStorageTelemetry(runtimeStorageTelemetryBridge{client: client}); err != nil {
		return nil, false, err
	}
	return client, true, nil
}

type runtimeStorageTelemetryBridge struct {
	client *resources.RuntimeClient
}

func (b runtimeStorageTelemetryBridge) ReportStorageIOTotals(readBytes, writeBytes, errorsTotal uint64, ingest storage.IngestSnapshot) error {
	if b.client == nil {
		return errors.New("runtime storage telemetry is unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return b.client.ReportProcessTelemetry(ctx, readBytes, writeBytes, errorsTotal, ingest.QueueBytes, ingest.OldestPersistAgeSeconds)
}

func (b runtimeStorageTelemetryBridge) StorageTelemetrySnapshot() (storage.RuntimeStorageTelemetrySnapshot, error) {
	if b.client == nil {
		return storage.RuntimeStorageTelemetrySnapshot{}, errors.New("runtime storage telemetry is unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resourcesSnapshot, err := b.client.Snapshot(ctx)
	if err != nil {
		return storage.RuntimeStorageTelemetrySnapshot{}, err
	}
	snapshot, err := b.client.TelemetrySnapshot(ctx)
	if err != nil {
		return storage.RuntimeStorageTelemetrySnapshot{}, err
	}
	result := storage.RuntimeStorageTelemetrySnapshot{
		Throughput: storage.PoolThroughput{
			ReadBytesPerSecond: snapshot.Throughput.ReadBytesPerSecond, WriteBytesPerSecond: snapshot.Throughput.WriteBytesPerSecond,
			ReadBytesTotal: snapshot.Throughput.ReadBytesTotal, WriteBytesTotal: snapshot.Throughput.WriteBytesTotal,
		},
		EstimatedCeiling: storage.EstimatedCeiling{
			ReadBytesPerSecond: snapshot.EstimatedCeiling.ReadBytesPerSecond, WriteBytesPerSecond: snapshot.EstimatedCeiling.WriteBytesPerSecond,
			Source: snapshot.EstimatedCeiling.Source,
		},
		ErrorsTotal: snapshot.ErrorsTotal,
		Samples:     make([]storage.PoolSample, 0, len(snapshot.Samples)),
		Ingest: storage.IngestSnapshot{
			BufferCapacityBytes:       resourcesSnapshot.Limits.GlobalBufferBytes,
			PerRecordingCapacityBytes: resourcesSnapshot.Limits.PerRecordingBufferBytes,
			BufferUsedBytes:           resourcesSnapshot.UsedBytes, ReservedBytes: resourcesSnapshot.UsedBytes,
			QueueObjects: resourcesSnapshot.QueueObjects, QueueBytes: resourcesSnapshot.QueueBytes,
			OldestPersistAgeSeconds: resourcesSnapshot.OldestAgeSeconds,
			ActiveWriters:           resourcesSnapshot.ActiveWriters,
			WriterConcurrency:       resourcesSnapshot.Limits.WriterConcurrency,
			StorageErrorsTotal:      snapshot.ErrorsTotal,
		},
	}
	for _, sample := range snapshot.Samples {
		result.Samples = append(result.Samples, storage.PoolSample{
			At: sample.At, ReadBytesPerSecond: sample.ReadBytesPerSecond,
			WriteBytesPerSecond: sample.WriteBytesPerSecond, BufferUsedBytes: sample.BufferUsedBytes,
			PersistQueueBytes: sample.PersistQueueBytes,
		})
	}
	return result, nil
}

func newInstanceID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", errors.New("engine instance identity unavailable")
	}
	return hex.EncodeToString(value[:]), nil
}

func engineInstanceID(getenv func(string) string) (string, error) {
	if getenv == nil {
		return "", errors.New("engine instance identity unavailable")
	}
	configured := strings.TrimSpace(getenv("ENGINE_INSTANCE_ID"))
	if configured == "" {
		return newInstanceID()
	}
	decoded, err := hex.DecodeString(configured)
	if err != nil || len(decoded) != 16 || strings.ToLower(configured) != configured {
		return "", errors.New("ENGINE_INSTANCE_ID is invalid")
	}
	return configured, nil
}
