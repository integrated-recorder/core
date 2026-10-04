package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

type fakeAdapterRuntime struct {
	mu           sync.Mutex
	descriptor   adapterproto.Descriptor
	check        func(int) (adapterproto.WatchCheckResult, error)
	checkContext func(context.Context, int) (adapterproto.WatchCheckResult, error)
	checkCount   int
	resolve      func() (adapterproto.MediaSource, error)
	resolveCount int
}

func (f *fakeAdapterRuntime) Descriptor(string) (adapterproto.Descriptor, error) {
	return f.descriptor, nil
}
func (f *fakeAdapterRuntime) ValidateResource(string, *adapterproto.ResourceRef) error { return nil }
func (f *fakeAdapterRuntime) ValidateWatchInput(_ string, input json.RawMessage, _ map[string]string, _ *adapterproto.ResourceRef) error {
	return adapterproto.ValidateObject(input)
}
func (f *fakeAdapterRuntime) WatchCheck(ctx context.Context, _ string, _ json.RawMessage, _ map[string]string, _ *adapterproto.ResourceRef) (adapterproto.WatchCheckResult, error) {
	f.mu.Lock()
	f.checkCount++
	count := f.checkCount
	check := f.check
	checkContext := f.checkContext
	f.mu.Unlock()
	if checkContext != nil {
		return checkContext(ctx, count)
	}
	if check == nil {
		return adapterproto.WatchCheckResult{State: "offline"}, nil
	}
	return check(count)
}
func (f *fakeAdapterRuntime) ResolveLegacyWithInputSecrets(_ context.Context, _ string, _ json.RawMessage, _ map[string]string, _ *adapterproto.ResourceRef) (adapterproto.MediaSource, error) {
	f.mu.Lock()
	f.resolveCount++
	resolve := f.resolve
	f.mu.Unlock()
	if resolve == nil {
		return testMedia(), nil
	}
	return resolve()
}
func (f *fakeAdapterRuntime) Provenance(string) (adapterproto.AdapterProvenance, error) {
	return adapterproto.AdapterProvenance{ID: "fixture", Version: "1", ProtocolVersion: adapterproto.Version}, nil
}
func (f *fakeAdapterRuntime) Get(string) (adapterhost.Adapter, error) {
	descriptor := f.descriptor
	return adapterhost.Adapter{Descriptor: &descriptor, Status: adapterhost.Status{ID: descriptor.ID, State: "ready"}}, nil
}

func (f *fakeAdapterRuntime) counts() (checks, resolves int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checkCount, f.resolveCount
}

type fakeRecordingManager struct {
	mu        sync.Mutex
	records   map[string]*domain.Recording
	startIDs  []string
	startErr  error
	startHook func()
}

func newFakeRecordingManager() *fakeRecordingManager {
	return &fakeRecordingManager{records: map[string]*domain.Recording{}}
}
func (m *fakeRecordingManager) StartResolvedWithID(_ context.Context, id, adapterID string, _ adapterproto.MediaSource, resource *adapterproto.ResourceRef, title string, provenance *adapterproto.AdapterProvenance) (*domain.Recording, error) {
	if m.startHook != nil {
		m.startHook()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startErr != nil {
		return nil, m.startErr
	}
	if _, exists := m.records[id]; exists {
		return nil, fmt.Errorf("duplicate id")
	}
	now := time.Now().UTC()
	recording := &domain.Recording{ID: id, AdapterID: adapterID, Title: title, State: domain.StateRecording, StartedAt: now, CreatedAt: now, Tracks: map[string]*domain.Track{}}
	if resource != nil {
		recording.Resource = &domain.ResourceReference{Type: resource.Type, ID: resource.ID}
	}
	if provenance != nil {
		recording.Adapter = &domain.AdapterProvenance{ID: provenance.ID, Version: provenance.Version, ProtocolVersion: provenance.ProtocolVersion}
	}
	m.records[id] = recording
	m.startIDs = append(m.startIDs, id)
	return cloneRecording(recording), nil
}
func (m *fakeRecordingManager) Get(id string) (*domain.Recording, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	recording := m.records[id]
	if recording == nil {
		return nil, storage.ErrNotFound
	}
	return cloneRecording(recording), nil
}
func (m *fakeRecordingManager) List() []*domain.Recording {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*domain.Recording, 0, len(m.records))
	for _, recording := range m.records {
		out = append(out, cloneRecording(recording))
	}
	return out
}
func (m *fakeRecordingManager) setState(id string, state domain.RecordingState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if recording := m.records[id]; recording != nil {
		recording.State = state
	}
}
func (m *fakeRecordingManager) startedIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.startIDs...)
}
func cloneRecording(recording *domain.Recording) *domain.Recording {
	copy := *recording
	return &copy
}

func newTestService(t *testing.T, adapter *fakeAdapterRuntime, manager *fakeRecordingManager, options Options) (*Service, View) {
	t.Helper()
	if adapter.descriptor.ID == "" {
		adapter.descriptor = adapterproto.Descriptor{ID: "fixture", Name: "Fixture", Version: "1", ProtocolVersion: adapterproto.Version, Capabilities: []string{adapterproto.CapabilityWatch, adapterproto.CapabilityResolve}, MediaTypes: []string{"hls"}, InputSchema: adapterproto.Schema{Fields: []adapterproto.Field{}}, ConfigurationSchema: adapterproto.Schema{Fields: []adapterproto.Field{}}}
	}
	service, err := New(t.TempDir(), adapter, manager, options)
	if err != nil {
		t.Fatal(err)
	}
	view, err := service.Create(Update{AdapterID: adapter.descriptor.ID, Input: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	return service, view
}

func testMedia() adapterproto.MediaSource {
	return adapterproto.MediaSource{Type: "hls", ManifestURL: "https://example.test/live.m3u8"}
}

func TestWatchCheckMediaFastPathAndRepeatedSessionDoesNotDuplicate(t *testing.T) {
	adapter := &fakeAdapterRuntime{check: func(int) (adapterproto.WatchCheckResult, error) {
		media := testMedia()
		return adapterproto.WatchCheckResult{State: "live", SessionRef: "session-A", Title: "Live", Media: &media}, nil
	}}
	manager := newFakeRecordingManager()
	service, view := newTestService(t, adapter, manager, Options{Jitter: func(time.Duration) time.Duration { return 0 }})
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	checks, resolves := adapter.counts()
	if checks != 1 || resolves != 0 {
		t.Fatalf("adapter calls checks=%d resolves=%d; want 1 and 0", checks, resolves)
	}
	runtime := getRuntime(t, service, view.ID)
	if runtime.State != StateRecording || runtime.ActiveRecordingID == "" || runtime.PendingRecordingID != "" {
		t.Fatalf("runtime after start = %#v", runtime)
	}
	if got := len(manager.startedIDs()); got != 1 {
		t.Fatalf("started recordings = %d, want 1", got)
	}
}

func TestPauseAndResumeFenceChecksDuringControlHandoff(t *testing.T) {
	entered := make(chan struct{})
	adapter := &fakeAdapterRuntime{checkContext: func(ctx context.Context, count int) (adapterproto.WatchCheckResult, error) {
		if count == 1 {
			close(entered)
			<-ctx.Done()
			return adapterproto.WatchCheckResult{}, ctx.Err()
		}
		return adapterproto.WatchCheckResult{State: "offline"}, nil
	}}
	service, view := newTestService(t, adapter, newFakeRecordingManager(), Options{})
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- service.Run(runCtx) }()
	<-service.runStarted
	job, err := service.enqueue(view.ID)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("watch check did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Pause(ctx); err != nil {
		t.Fatalf("pause: %v", err)
	}
	select {
	case <-job.done:
	case <-time.After(time.Second):
		t.Fatal("in-flight check did not finish before pause returned")
	}
	if _, err := service.enqueue(view.ID); !errors.Is(err, ErrHandoffPaused) {
		t.Fatalf("enqueue while paused = %v", err)
	}
	if err := service.Resume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if err := service.ManualCheck(context.Background(), view.ID); err != nil {
		t.Fatalf("check after resume: %v", err)
	}
	checks, _ := adapter.counts()
	if checks != 2 {
		t.Fatalf("adapter check count = %d; want 2", checks)
	}
	cancelRun()
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("scheduler exit = %v", err)
	}
}

func TestWatchResolveFallbackExactlyOnce(t *testing.T) {
	adapter := &fakeAdapterRuntime{check: func(int) (adapterproto.WatchCheckResult, error) {
		return adapterproto.WatchCheckResult{State: "live", SessionRef: "session-A"}, nil
	}}
	manager := newFakeRecordingManager()
	service, view := newTestService(t, adapter, manager, Options{})
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	checks, resolves := adapter.counts()
	if checks != 1 || resolves != 1 || len(manager.startedIDs()) != 1 {
		t.Fatalf("calls checks=%d resolves=%d recordings=%d; want 1, 1, 1", checks, resolves, len(manager.startedIDs()))
	}
}

func TestOfflineAndAdapterErrorRemainDistinct(t *testing.T) {
	adapter := &fakeAdapterRuntime{check: func(n int) (adapterproto.WatchCheckResult, error) {
		if n == 1 {
			return adapterproto.WatchCheckResult{State: "offline"}, nil
		}
		return adapterproto.WatchCheckResult{}, errors.New("private adapter error")
	}}
	manager := newFakeRecordingManager()
	service, view := newTestService(t, adapter, manager, Options{Jitter: func(time.Duration) time.Duration { return 0 }})
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	if got := getRuntime(t, service, view.ID).State; got != StateOffline {
		t.Fatalf("offline state = %q", got)
	}
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	runtime := getRuntime(t, service, view.ID)
	if runtime.State != StateBackoff || runtime.LastErrorCode != "check_failed" {
		t.Fatalf("error state = %#v; want backoff/check_failed", runtime)
	}
	if strings.Contains(runtime.LastErrorCode, "private") || len(manager.startedIDs()) != 0 {
		t.Fatal("raw adapter error leaked or a recording was started")
	}
}

func TestAllowlistedAdapterErrorsRequireAttentionWithoutPersistingRemoteText(t *testing.T) {
	for _, code := range []string{"authentication_required", "interaction_required", "configuration_required"} {
		t.Run(code, func(t *testing.T) {
			adapter := &fakeAdapterRuntime{check: func(int) (adapterproto.WatchCheckResult, error) {
				return adapterproto.WatchCheckResult{}, fmt.Errorf("remote body leaked-secret-sentinel: %w", &adapterhost.SafeProtocolError{Code: code})
			}}
			manager := newFakeRecordingManager()
			service, view := newTestService(t, adapter, manager, Options{Jitter: func(time.Duration) time.Duration { return 0 }})
			if err := service.runCheck(context.Background(), view.ID); err != nil {
				t.Fatal(err)
			}
			runtime := getRuntime(t, service, view.ID)
			if runtime.State != StateAttentionRequired || runtime.LastErrorCode != code || runtime.FailureCount != 1 || runtime.NextCheckAt == nil {
				t.Fatalf("runtime = %#v; want attention_required/%s with bounded retry", runtime, code)
			}
			if got := len(manager.startedIDs()); got != 0 {
				t.Fatalf("recordings started after attention error = %d", got)
			}
			publicView, err := service.Get(view.ID)
			if err != nil {
				t.Fatal(err)
			}
			events, err := service.Events(view.ID, 10)
			if err != nil {
				t.Fatal(err)
			}
			for name, value := range map[string]any{"runtime": runtime, "view": publicView, "events": events} {
				encoded, marshalErr := json.Marshal(value)
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				if strings.Contains(string(encoded), "leaked-secret-sentinel") || strings.Contains(string(encoded), "remote body") {
					t.Fatalf("%s persisted remote adapter error text: %s", name, encoded)
				}
			}
			found := map[string]bool{}
			for _, event := range events {
				if event.Code == code && (event.Type == "check_failed" || event.Type == "attention_required") {
					found[event.Type] = true
				}
			}
			if !found["check_failed"] || !found["attention_required"] {
				t.Fatalf("safe attention events = %#v", events)
			}
		})
	}
}

func TestCompletedSameSessionIsSuppressed(t *testing.T) {
	adapter := &fakeAdapterRuntime{check: func(int) (adapterproto.WatchCheckResult, error) {
		media := testMedia()
		return adapterproto.WatchCheckResult{State: "live", SessionRef: "session-A", Media: &media}, nil
	}}
	manager := newFakeRecordingManager()
	service, view := newTestService(t, adapter, manager, Options{})
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	ids := manager.startedIDs()
	manager.setState(ids[0], domain.StateCompleted)
	def, runtime, _, err := service.store.Get(view.ID)
	if err != nil {
		t.Fatal(err)
	}
	service.reconcileOne(def, runtime, time.Now().UTC())
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	if got := len(manager.startedIDs()); got != 1 {
		t.Fatalf("same completed session started %d recordings, want 1", got)
	}

}

func TestInterruptedSameSessionStartsNewPart(t *testing.T) {
	adapter := &fakeAdapterRuntime{check: func(int) (adapterproto.WatchCheckResult, error) {
		media := testMedia()
		return adapterproto.WatchCheckResult{State: "live", SessionRef: "session-A", Media: &media}, nil
	}}
	manager := newFakeRecordingManager()
	service, view := newTestService(t, adapter, manager, Options{})
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	firstID := manager.startedIDs()[0]
	manager.setState(firstID, domain.StateInterrupted)
	def, runtime, _, err := service.store.Get(view.ID)
	if err != nil {
		t.Fatal(err)
	}
	service.reconcileOne(def, runtime, time.Now().UTC())
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	ids := manager.startedIDs()
	if len(ids) != 2 {
		t.Fatalf("interrupted same session starts = %d, want 2", len(ids))
	}
	relations, err := service.Relations(view.ID, 10)
	if err != nil || len(relations) != 2 {
		t.Fatalf("interrupted part relations = %#v, error = %v", relations, err)
	}
	parts := map[int]bool{relations[0].PartIndex: true, relations[1].PartIndex: true}
	if !parts[1] || !parts[2] {
		t.Fatalf("part indexes = %#v, want 1 and 2", parts)
	}
}

func TestManualStopSuppressesCurrentSessionUntilOffline(t *testing.T) {
	state := "live"
	adapter := &fakeAdapterRuntime{check: func(int) (adapterproto.WatchCheckResult, error) {
		if state == "offline" {
			return adapterproto.WatchCheckResult{State: "offline"}, nil
		}
		media := testMedia()
		return adapterproto.WatchCheckResult{State: "live", SessionRef: "session-A", Media: &media}, nil
	}}
	manager := newFakeRecordingManager()
	service, view := newTestService(t, adapter, manager, Options{})
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	id := manager.startedIDs()[0]
	service.NotifyRecordingStopped(id)
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	if got := len(manager.startedIDs()); got != 1 {
		t.Fatalf("same manually stopped session started %d recordings, want 1", got)
	}
	state = "offline"
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	state = "live"
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	if got := len(manager.startedIDs()); got != 2 {
		t.Fatalf("new session after observed offline started %d recordings, want 2", got)
	}
}

func TestManualStopSuppressionReleasesOnNewSessionAfterUnknownSession(t *testing.T) {
	ref := ""
	state := "live"
	adapter := &fakeAdapterRuntime{check: func(int) (adapterproto.WatchCheckResult, error) {
		if state == "offline" {
			return adapterproto.WatchCheckResult{State: "offline"}, nil
		}
		media := testMedia()
		return adapterproto.WatchCheckResult{State: "live", SessionRef: ref, Media: &media}, nil
	}}
	manager := newFakeRecordingManager()
	service, view := newTestService(t, adapter, manager, Options{})
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	service.NotifyRecordingStopped(manager.startedIDs()[0])
	ref = "new-session-A"
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	if got := len(manager.startedIDs()); got != 1 {
		t.Fatalf("first session ref after unidentifiable stop started %d recordings, want suppression until offline", got)
	}
	state = "offline"
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	state = "live"
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	if got := len(manager.startedIDs()); got != 2 {
		t.Fatalf("live after offline observation started %d recordings, want 2", got)
	}
}

func TestDisableAndDeleteNeverStopAnActiveRecording(t *testing.T) {
	adapter := &fakeAdapterRuntime{check: func(int) (adapterproto.WatchCheckResult, error) {
		media := testMedia()
		return adapterproto.WatchCheckResult{State: "live", SessionRef: "session-A", Media: &media}, nil
	}}
	manager := newFakeRecordingManager()
	service, view := newTestService(t, adapter, manager, Options{})
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatal(err)
	}
	id := manager.startedIDs()[0]
	if _, err := service.SetEnabled(view.ID, false); err != nil {
		t.Fatal(err)
	}
	if recording, err := manager.Get(id); err != nil || recording.State != domain.StateRecording {
		t.Fatalf("disable affected active recording: recording=%#v err=%v", recording, err)
	}
	if err := service.Delete(view.ID); err != nil {
		t.Fatal(err)
	}
	if recording, err := manager.Get(id); err != nil || recording.State != domain.StateRecording {
		t.Fatalf("delete affected active recording: recording=%#v err=%v", recording, err)
	}
}

func TestStartHandoffDoesNotBlockOtherWatchesAndDisableWaitsForCurrentStart(t *testing.T) {
	adapter := &fakeAdapterRuntime{check: func(int) (adapterproto.WatchCheckResult, error) {
		media := testMedia()
		return adapterproto.WatchCheckResult{State: "live", SessionRef: "session-A", Media: &media}, nil
	}}
	manager := newFakeRecordingManager()
	started := make(chan struct{})
	release := make(chan struct{})
	manager.startHook = func() { close(started); <-release }
	service, first := newTestService(t, adapter, manager, Options{})
	second, err := service.Create(Update{AdapterID: adapter.descriptor.ID, Input: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	startDone := make(chan error, 1)
	go func() { startDone <- service.runCheck(context.Background(), first.ID) }()
	<-started

	firstDisabled := make(chan error, 1)
	go func() { _, err := service.SetEnabled(first.ID, false); firstDisabled <- err }()
	deadline := time.Now().Add(time.Second)
	for {
		service.mu.Lock()
		gate := service.startGates[first.ID]
		waiting := gate != nil && gate.refs >= 2
		service.mu.Unlock()
		if waiting {
			break
		}
		select {
		case err := <-firstDisabled:
			t.Fatalf("disable passed a start handoff that was already underway: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("disable did not wait for the per-Watch recording handoff")
		}
		runtime.Gosched()
	}
	secondDisabled := make(chan error, 1)
	go func() { _, err := service.SetEnabled(second.ID, false); secondDisabled <- err }()
	select {
	case err := <-secondDisabled:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("a slow start handoff blocked an unrelated Watch")
	}
	close(release)
	if err := <-startDone; err != nil {
		t.Fatal(err)
	}
	if err := <-firstDisabled; err != nil {
		t.Fatal(err)
	}
	ids := manager.startedIDs()
	if len(ids) != 1 {
		t.Fatalf("recordings started = %d, want 1", len(ids))
	}
	if recording, err := manager.Get(ids[0]); err != nil || recording.State != domain.StateRecording {
		t.Fatalf("disable stopped in-flight recording: %#v, %v", recording, err)
	}
	if state := getRuntime(t, service, first.ID).State; state != StateRecording {
		t.Fatalf("watch state after disable handoff = %q, want recording", state)
	}
}

func TestConcurrentDueManualChecksCoalesceToOneAdapterCallAndRecording(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	adapter := &fakeAdapterRuntime{checkContext: func(ctx context.Context, _ int) (adapterproto.WatchCheckResult, error) {
		close(started)
		select {
		case <-release:
			media := testMedia()
			return adapterproto.WatchCheckResult{State: "live", SessionRef: "session-A", Media: &media}, nil
		case <-ctx.Done():
			return adapterproto.WatchCheckResult{}, ctx.Err()
		}
	}}
	manager := newFakeRecordingManager()
	service, view := newTestService(t, adapter, manager, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- service.Run(ctx) }()
	<-service.runStarted
	if got := service.ScheduleDueAt(time.Now().Add(time.Hour)); got != 1 {
		cancel()
		<-runDone
		t.Fatalf("first due queue count = %d", got)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		cancel()
		<-runDone
		t.Fatal("scheduled adapter check did not start")
	}
	for i := 0; i < 10; i++ {
		service.ScheduleDueAt(time.Now().Add(time.Hour))
	}
	service.mu.Lock()
	firstJob := service.jobs[view.ID]
	service.mu.Unlock()
	coalescedJob, err := service.enqueue(view.ID)
	if err != nil || coalescedJob != firstJob {
		t.Fatalf("manual/scheduled check did not coalesce: job=%p first=%p err=%v", coalescedJob, firstJob, err)
	}
	close(release)
	select {
	case <-firstJob.done:
		if firstJob.err != nil {
			t.Fatal(firstJob.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("coalesced check did not finish")
	}
	checks, _ := adapter.counts()
	if checks != 1 || len(manager.startedIDs()) != 1 {
		t.Fatalf("coalesced work checks=%d recordings=%d, want one each", checks, len(manager.startedIDs()))
	}
	cancel()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer closeCancel()
	if err := service.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want cancellation", err)
	}
}

func TestRunCloseJoinsBlockedAdapterCheck(t *testing.T) {
	started := make(chan struct{})
	adapter := &fakeAdapterRuntime{checkContext: func(ctx context.Context, _ int) (adapterproto.WatchCheckResult, error) {
		close(started)
		<-ctx.Done()
		return adapterproto.WatchCheckResult{}, ctx.Err()
	}}
	service, view := newTestService(t, adapter, newFakeRecordingManager(), Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- service.Run(ctx) }()
	<-service.runStarted
	if got := service.ScheduleDueAt(time.Now().Add(time.Hour)); got != 1 {
		cancel()
		<-runDone
		t.Fatalf("due queue count = %d", got)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		cancel()
		<-runDone
		t.Fatal("adapter check did not start")
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer closeCancel()
	if err := service.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want cancellation", err)
	}
	if _, err := service.Get(view.ID); err != nil {
		t.Fatalf("Watch disappeared after joined close: %v", err)
	}
}

func TestBackoffIsBoundedAfterJitter(t *testing.T) {
	maximumJitter := func(bound time.Duration) time.Duration { return bound }
	if got := backoffDelay(99, maximumJitter); got != 300*time.Second {
		t.Fatalf("max backoff = %s, want 5m", got)
	}
}

func TestScheduleDueUsesSnapshotsWithoutSecretReads(t *testing.T) {
	adapter := &fakeAdapterRuntime{}
	manager := newFakeRecordingManager()
	service, view := newTestService(t, adapter, manager, Options{})
	if err := os.WriteFile(service.store.secretPath(view.ID), []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Scheduling is metadata-only. Secret files are loaded only when a due
	// worker starts the adapter call, never on each scheduler tick.
	service.running = true
	if got := service.ScheduleDueAt(time.Now().Add(time.Hour)); got != 1 {
		t.Fatalf("queued due watches = %d, want 1", got)
	}
}

func TestRecoverExactPendingRecordingAndReapplyPreviewMode(t *testing.T) {
	adapter := &fakeAdapterRuntime{}
	manager := newFakeRecordingManager()
	previewCalls := 0
	service, view := newTestService(t, adapter, manager, Options{Preview: func(recordingID, mode string) error {
		previewCalls++
		if recordingID == "" || mode != "disabled" {
			t.Fatalf("recovered preview policy args = %q, %q", recordingID, mode)
		}
		return nil
	}})
	pendingID, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	recording, err := manager.StartResolvedWithID(context.Background(), pendingID, "fixture", testMedia(), nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, runtime, _, err := service.store.Get(view.ID)
	if err != nil {
		t.Fatal(err)
	}
	runtime.State = StateStarting
	runtime.PendingRecordingID = pendingID
	if err := service.store.SaveRuntime(view.ID, runtime); err != nil {
		t.Fatal(err)
	}
	service.recoverLinks()
	runtime = getRuntime(t, service, view.ID)
	if runtime.ActiveRecordingID != recording.ID || runtime.PendingRecordingID != "" || runtime.State != StateRecording {
		t.Fatalf("recovered runtime = %#v", runtime)
	}
	if previewCalls != 1 {
		t.Fatalf("recovery reapplied preview policy %d times, want 1", previewCalls)
	}
}

func getRuntime(t *testing.T, service *Service, id string) Runtime {
	t.Helper()
	_, runtime, _, err := service.store.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func TestUpdateCanClearResourceAndPreservesPartialUpdateSemantics(t *testing.T) {
	adapter := &fakeAdapterRuntime{}
	service, _ := newTestService(t, adapter, newFakeRecordingManager(), Options{})
	resource := &adapterproto.ResourceRef{Type: "channel", ID: "demo"}
	view, err := service.Create(Update{AdapterID: adapter.descriptor.ID, Input: json.RawMessage(`{}`), Resource: resource})
	if err != nil {
		t.Fatal(err)
	}

	// A partial update without clear_resource continues to preserve the
	// existing selection, matching the historical API behavior.
	view, err = service.Update(view.ID, Update{})
	if err != nil {
		t.Fatal(err)
	}
	if view.Resource == nil || view.Resource.Type != resource.Type || view.Resource.ID != resource.ID {
		t.Fatalf("partial update resource = %#v, want existing resource", view.Resource)
	}

	runtime := getRuntime(t, service, view.ID)
	runtime.SessionDigest = strings.Repeat("a", 64)
	runtime.SuppressedSessionDigest = strings.Repeat("b", 64)
	runtime.Suppressed = true
	if err := service.store.SaveRuntime(view.ID, runtime); err != nil {
		t.Fatal(err)
	}

	view, err = service.Update(view.ID, Update{ClearResource: true})
	if err != nil {
		t.Fatal(err)
	}
	if view.Resource != nil {
		t.Fatalf("updated API view retained cleared resource: %#v", view.Resource)
	}
	definition, persistedRuntime, _, err := service.store.Get(view.ID)
	if err != nil {
		t.Fatal(err)
	}
	if definition.Resource != nil {
		t.Fatalf("durable definition retained cleared resource: %#v", definition.Resource)
	}
	if persistedRuntime.SessionDigest != "" || persistedRuntime.SuppressedSessionDigest != "" || persistedRuntime.Suppressed {
		t.Fatalf("source-change runtime was not reset after resource clear: %#v", persistedRuntime)
	}
	view, err = service.Get(view.ID)
	if err != nil || view.Resource != nil {
		t.Fatalf("reloaded API view resource=%#v err=%v", view.Resource, err)
	}

	// An explicit replacement takes precedence if a client sends both fields.
	replacement := &adapterproto.ResourceRef{Type: "channel", ID: "next"}
	view, err = service.Update(view.ID, Update{Resource: replacement, ClearResource: true})
	if err != nil {
		t.Fatal(err)
	}
	if view.Resource == nil || view.Resource.ID != replacement.ID {
		t.Fatalf("non-nil replacement did not win over clear_resource: %#v", view.Resource)
	}
}
