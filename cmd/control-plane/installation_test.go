package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/preview"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/runtimehost/installation"
	"github.com/integrated-recorder/core/internal/server"
	"github.com/integrated-recorder/core/internal/storage"
	"github.com/integrated-recorder/core/internal/systemsettings"
	"github.com/integrated-recorder/core/internal/watch"
)

func TestControlBackgroundProducersStayStoppedUntilHostInstallationReady(t *testing.T) {
	base := os.TempDir()
	if resolved, resolveErr := filepath.EvalSymlinks(base); resolveErr == nil {
		base = resolved
	}
	root, err := os.MkdirTemp(base, "control-installation-gate-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	state, err := installation.Reconcile(root, installation.AdminMissing, false)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &installationGateAdapter{}
	watchStore, err := watch.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	watchID := "0123456789abcdef0123456789abcdef"
	dueAt := now.Add(-time.Second)
	if err := watchStore.Create(watch.Definition{
		ID: watchID, AdapterID: "fixture", Input: json.RawMessage(`{}`), Enabled: true,
		PreviewMode: "disabled", CheckIntervalSeconds: watch.MinIntervalSeconds, CreatedAt: now, UpdatedAt: now,
	}, nil, watch.Runtime{State: watch.StateChecking, NextCheckAt: &dueAt}); err != nil {
		t.Fatal(err)
	}
	archiveStore, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManagerWithMode(archiveStore, nil, nil, nil, acquire.FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := systemsettings.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	previewService, err := preview.OpenWithGet(root, archiveStore, manager.List, manager.Get, "")
	if err != nil {
		t.Fatal(err)
	}
	watchService, err := watch.New(root, adapter, manager, watch.Options{
		Now: func() time.Time { return time.Now().UTC() }, Jitter: func(time.Duration) time.Duration { return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	api := server.NewWithOptions(manager, nil, nil, server.Options{
		Storage: archiveStore, Management: products, Settings: settings, Previews: previewService, Watches: watchService,
	})
	app := &controlApplication{
		dataDir: root, installationManaged: true, lifetime: context.Background(), store: archiveStore,
		previews: previewService, watches: watchService, api: api,
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		app.mu.Lock()
		backgroundCancel, backgroundDone := app.backgroundCancel, app.backgroundDone
		app.mu.Unlock()
		if backgroundCancel != nil {
			backgroundCancel()
		}
		_ = app.stopPreviewReconciler(ctx)
		_ = app.stopRetentionLoop(ctx)
		_ = app.stopStorageMetricsLoop(ctx)
		if backgroundDone != nil {
			select {
			case <-backgroundDone:
			case <-ctx.Done():
				t.Error("Control Watch background loop did not stop")
			}
		}
		_ = watchService.Close(ctx)
		_ = previewService.Close(ctx)
		_ = manager.Close(ctx)
	})
	if views := watchService.List(); len(views) != 1 || views[0].Definition.ID != watchID || !views[0].Enabled {
		t.Fatalf("expected a durable enabled Watch before setup completion, got %+v", views)
	}
	if err := app.startBackground(context.Background()); err != nil {
		t.Fatal(err)
	}
	app.mu.Lock()
	started := app.backgroundCancel != nil || app.previewCancel != nil || app.retentionCancel != nil || app.storageMetricsCancel != nil
	app.mu.Unlock()
	if started {
		t.Fatal("Control started a management producer before Host installation readiness")
	}
	if got := adapter.checks.Load(); got != 0 {
		t.Fatalf("setup-incomplete Control polled a persisted enabled Watch %d times", got)
	}
	if state.Snapshot().State != installation.StateUninitialized {
		t.Fatalf("unexpected initial state=%s", state.Snapshot().State)
	}
	if _, err := state.Begin(true); err != nil {
		t.Fatal(err)
	}
	if _, err := state.Complete(); err != nil {
		t.Fatal(err)
	}
	if !app.installationIsReady() {
		t.Fatal("Control did not observe Host's durable ready transition")
	}
	if err := app.installationReady(context.Background()); err != nil {
		t.Fatalf("Control could not activate management services after ready transition: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for adapter.checks.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := adapter.checks.Load(); got == 0 {
		t.Fatal("Watch scheduler did not begin polling after Host installation-ready signal")
	}
	app.mu.Lock()
	startedAfterReady := app.backgroundCancel != nil && app.previewCancel != nil && app.retentionCancel != nil && app.storageMetricsCancel != nil
	app.mu.Unlock()
	if !startedAfterReady {
		t.Fatal("Host installation-ready signal did not start all Control background producers")
	}
	// Open a second Control projection as a restart-equivalent check: readiness
	// is shared durable Host state, never generation-local memory.
	reloaded := installation.ReadOnly(filepath.Clean(root))
	if reloaded.State != installation.StateReady || reloaded.InstallationID != state.Snapshot().InstallationID {
		t.Fatalf("restart-equivalent installation snapshot=%+v", reloaded)
	}
}

func TestValidateInstallationAllowsUnknownCapacityWhenProviderProbePasses(t *testing.T) {
	objects := newSetupValidationObjects()
	store, err := storage.NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	app := &controlApplication{store: store, manager: &recorderengine.ManagerRouter{}, adapters: &adapterhost.Host{}}
	if err := app.validateInstallation(context.Background()); err != nil {
		t.Fatalf("successful provider write/durability probe with unknown capacity was rejected: %v", err)
	}
	result := store.RunSetupProbe()
	if !result.WritePassed || !result.DurabilityPassed || result.CapacityKnown {
		t.Fatalf("expected successful probe and unknown provider capacity, got %+v", result)
	}
}

func TestValidateInstallationRejectsFailedProviderProbe(t *testing.T) {
	objects := newSetupValidationObjects()
	objects.failPut = true
	store, err := storage.NewWithObjectStore(t.TempDir(), objects)
	if err != nil {
		t.Fatal(err)
	}
	app := &controlApplication{store: store, manager: &recorderengine.ManagerRouter{}, adapters: &adapterhost.Host{}}
	if err := app.validateInstallation(context.Background()); err == nil {
		t.Fatal("failed physical provider probe was accepted")
	}
}

type setupValidationObjects struct {
	mu      sync.Mutex
	objects map[string][]byte
	failPut bool
}

func newSetupValidationObjects() *setupValidationObjects {
	return &setupValidationObjects{objects: make(map[string][]byte)}
}

func (s *setupValidationObjects) Put(ctx context.Context, key string, body io.Reader, size int64) (storage.PhysicalObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return storage.PhysicalObjectInfo{}, err
	}
	if s.failPut {
		return storage.PhysicalObjectInfo{}, errors.New("injected provider failure")
	}
	data, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil || size < 0 || int64(len(data)) != size {
		return storage.PhysicalObjectInfo{}, errors.New("invalid provider payload")
	}
	s.mu.Lock()
	s.objects[key] = append([]byte(nil), data...)
	info := s.infoLocked(key)
	s.mu.Unlock()
	return info, nil
}

func (s *setupValidationObjects) Open(ctx context.Context, key string) (io.ReadCloser, storage.PhysicalObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, storage.PhysicalObjectInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		return nil, storage.PhysicalObjectInfo{}, storage.ErrObjectNotFound
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), data...))), s.infoLocked(key), nil
}

func (s *setupValidationObjects) OpenRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.PhysicalObjectInfo, error) {
	reader, info, err := s.Open(ctx, key)
	if err != nil {
		return nil, storage.PhysicalObjectInfo{}, err
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || offset < 0 || length < 0 || offset > int64(len(data))-length {
		return nil, storage.PhysicalObjectInfo{}, errors.New("invalid provider range")
	}
	return io.NopCloser(bytes.NewReader(data[offset : offset+length])), info, nil
}

func (s *setupValidationObjects) Stat(ctx context.Context, key string) (storage.PhysicalObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return storage.PhysicalObjectInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[key]; !ok {
		return storage.PhysicalObjectInfo{}, storage.ErrObjectNotFound
	}
	return s.infoLocked(key), nil
}

func (s *setupValidationObjects) List(ctx context.Context, prefix, cursor string, limit int) (storage.PhysicalObjectPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.PhysicalObjectPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix && key > cursor {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if limit < 1 {
		return storage.PhysicalObjectPage{}, errors.New("invalid list limit")
	}
	page := storage.PhysicalObjectPage{}
	if len(keys) > limit {
		page.NextCursor = keys[limit-1]
		keys = keys[:limit]
	}
	for _, key := range keys {
		page.Items = append(page.Items, s.infoLocked(key))
	}
	return page, nil
}

func (s *setupValidationObjects) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[key]; !ok {
		return storage.ErrObjectNotFound
	}
	delete(s.objects, key)
	return nil
}

func (s *setupValidationObjects) infoLocked(key string) storage.PhysicalObjectInfo {
	data := s.objects[key]
	digest := sha256.Sum256(data)
	return storage.PhysicalObjectInfo{Key: key, Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:]), ModifiedAt: time.Now().UTC()}
}

type installationGateAdapter struct{ checks atomic.Int32 }

func (a *installationGateAdapter) Descriptor(string) (adapterproto.Descriptor, error) {
	return adapterproto.Descriptor{ID: "fixture", Name: "Fixture", Version: "1", ProtocolVersion: adapterproto.Version, Capabilities: []string{adapterproto.CapabilityWatch}}, nil
}
func (*installationGateAdapter) ValidateResource(string, *adapterproto.ResourceRef) error { return nil }
func (*installationGateAdapter) ValidateWatchInput(string, json.RawMessage, map[string]string, *adapterproto.ResourceRef) error {
	return nil
}
func (a *installationGateAdapter) WatchCheck(context.Context, string, json.RawMessage, map[string]string, *adapterproto.ResourceRef) (adapterproto.WatchCheckResult, error) {
	a.checks.Add(1)
	return adapterproto.WatchCheckResult{State: "offline"}, nil
}
func (*installationGateAdapter) ResolveLegacyWithInputSecrets(context.Context, string, json.RawMessage, map[string]string, *adapterproto.ResourceRef) (adapterproto.MediaSource, error) {
	return adapterproto.MediaSource{}, nil
}
func (*installationGateAdapter) Provenance(string) (adapterproto.AdapterProvenance, error) {
	return adapterproto.AdapterProvenance{ID: "fixture", Version: "1", ProtocolVersion: adapterproto.Version}, nil
}
func (*installationGateAdapter) Get(id string) (adapterhost.Adapter, error) {
	descriptor := adapterproto.Descriptor{ID: id, Name: "Fixture", Version: "1", ProtocolVersion: adapterproto.Version, Capabilities: []string{adapterproto.CapabilityWatch}}
	return adapterhost.Adapter{Descriptor: &descriptor, Status: adapterhost.Status{ID: id, State: "ready"}}, nil
}
