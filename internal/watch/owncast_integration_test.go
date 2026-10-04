package watch

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/adapters/owncast"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

const owncastIntegrationPayload = "owncast-canonical-segment-bytes"

// owncastIntegrationAdapterRuntime exercises the real Owncast watch checker
// while keeping all other adapter-host behavior local to this integration
// test. In particular, its watch result is passed through Service unchanged.
type owncastIntegrationAdapterRuntime struct {
	*fakeAdapterRuntime
	client *http.Client
}

func newOwncastIntegrationAdapterRuntime(client *http.Client) *owncastIntegrationAdapterRuntime {
	return &owncastIntegrationAdapterRuntime{
		fakeAdapterRuntime: &fakeAdapterRuntime{descriptor: owncast.Describe()},
		client:             client,
	}
}

func (a *owncastIntegrationAdapterRuntime) WatchCheck(_ context.Context, _ string, input json.RawMessage, _ map[string]string, _ *adapterproto.ResourceRef) (adapterproto.WatchCheckResult, error) {
	a.mu.Lock()
	a.checkCount++
	a.mu.Unlock()
	return owncast.WatchCheckWith(input, a.client, func(context.Context, string) error { return nil })
}

func TestOwncastWatchStartsRealAcquisitionAndReturnsOfflineAfterEndList(t *testing.T) {
	var online atomic.Bool
	var endList atomic.Bool
	var statusRequests atomic.Int32
	var segmentRequests atomic.Int32
	online.Store(false)

	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/status":
			statusRequests.Add(1)
			if online.Load() {
				_, _ = io.WriteString(w, `{"online":true,"lastConnectTime":"2026-09-28T01:02:03Z"}`)
				return
			}
			_, _ = io.WriteString(w, `{"online":false}`)
		case owncast.StreamPath:
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			manifest := "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:1.0,\nsegment-1.ts\n"
			if endList.Load() {
				manifest += "#EXT-X-ENDLIST\n"
			}
			_, _ = io.WriteString(w, manifest)
		case "/hls/segment-1.ts":
			segmentRequests.Add(1)
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = io.WriteString(w, owncastIntegrationPayload)
		default:
			http.NotFound(w, r)
		}
	}))
	defer fixture.Close()

	root := t.TempDir()
	archive, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(archive, fixture.Client(), nil, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if closeErr := manager.Close(ctx); closeErr != nil {
			t.Errorf("close acquisition manager: %v", closeErr)
		}
	})

	adapter := newOwncastIntegrationAdapterRuntime(fixture.Client())
	service, err := New(root, adapter, manager, Options{Jitter: func(time.Duration) time.Duration { return 0 }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := service.Close(context.Background()); closeErr != nil {
			t.Errorf("close Watch service: %v", closeErr)
		}
	})

	input, err := json.Marshal(map[string]string{"source_url": fixture.URL + "/"})
	if err != nil {
		t.Fatal(err)
	}
	view, err := service.Create(Update{AdapterID: "owncast", Input: input, Title: stringPointer("Owncast integration")})
	if err != nil {
		t.Fatal(err)
	}
	if got := adapter.descriptor.Capabilities; !hasCapability(got, adapterproto.CapabilityWatch) || !hasCapability(got, adapterproto.CapabilityResolve) || len(adapter.descriptor.MediaTypes) != 1 || adapter.descriptor.MediaTypes[0] != "hls" {
		t.Fatalf("adapter descriptor does not match Owncast watch/resolve HLS capability: %#v", adapter.descriptor)
	}

	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatalf("initial real Owncast offline check: %v", err)
	}
	view, err = service.Get(view.ID)
	if err != nil || view.State != StateOffline || view.CurrentRecordingID != "" || len(manager.List()) != 0 {
		t.Fatalf("Watch after Owncast offline observation = %#v, err=%v; no Recording should exist", view, err)
	}

	// Transition the actual status endpoint to live and let the same durable
	// Watch start a recording through the normal StartResolved acquisition path.
	online.Store(true)
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatalf("real Owncast live check: %v", err)
	}
	runtime := getRuntime(t, service, view.ID)
	if runtime.State != StateRecording || runtime.ActiveRecordingID == "" {
		t.Fatalf("Watch state after live check = %#v, want active recording", runtime)
	}
	recordingID := runtime.ActiveRecordingID
	waitOwncastIntegration(t, "canonical segment capture", func() bool {
		recording, getErr := manager.Get(recordingID)
		return getErr == nil && recording.SegmentCount() >= 1
	})

	// ENDLIST is published only after the segment has been committed, so the
	// shared acquisition worker must drain the discovered segment before it
	// writes the terminal completed state.
	endList.Store(true)
	waitOwncastIntegration(t, "HLS ENDLIST completion", func() bool {
		recording, getErr := manager.Get(recordingID)
		return getErr == nil && recording.State == domain.StateCompleted
	})

	completed, err := manager.Get(recordingID)
	if err != nil {
		t.Fatal(err)
	}
	track := completed.Tracks["main"]
	if track == nil || len(track.Segments) != 1 {
		t.Fatalf("terminal recording tracks = %#v, want one archived segment", completed.Tracks)
	}
	segment := track.Segments[0]
	if segment.ArchiveOrdinal != 1 || segment.PayloadSize != int64(len(owncastIntegrationPayload)) {
		t.Fatalf("canonical segment metadata = %#v", segment)
	}
	payload, err := archive.OpenPayload(recordingID, segment.StoragePath)
	if err != nil {
		t.Fatalf("open canonical segment payload: %v", err)
	}
	payloadBytes, readErr := io.ReadAll(payload)
	closeErr := payload.Close()
	if readErr != nil || closeErr != nil || string(payloadBytes) != owncastIntegrationPayload {
		t.Fatalf("canonical segment bytes = %q; read error=%v close error=%v", payloadBytes, readErr, closeErr)
	}
	if segmentRequests.Load() < 1 {
		t.Fatal("fixture did not receive a canonical segment request")
	}

	// The relation is committed separately from recording.json. Reloading the
	// Watch store verifies that the recording-to-Watch projection is durable.
	reloadedWatchStore, err := Open(root)
	if err != nil {
		t.Fatalf("reload Watch store: %v", err)
	}
	relation, err := reloadedWatchStore.Relation(recordingID)
	if err != nil || relation.WatchID != view.ID || relation.RecordingID != recordingID || relation.PartIndex != 1 {
		t.Fatalf("durable Watch relation = %#v, err=%v", relation, err)
	}

	// The real acquisition manager owns ENDLIST completion. The Watch then
	// reconciles the terminal recording and makes its next check immediately
	// eligible; a second check observes Owncast offline and preserves the archive.
	online.Store(false)
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatalf("reconcile terminal recording: %v", err)
	}
	if err := service.runCheck(context.Background(), view.ID); err != nil {
		t.Fatalf("post-terminal Owncast offline check: %v", err)
	}
	view, err = service.Get(view.ID)
	if err != nil || view.State != StateOffline || view.CurrentRecordingID != "" {
		t.Fatalf("Watch after offline recheck = %#v, err=%v", view, err)
	}
	retained, err := manager.Get(recordingID)
	if err != nil || retained.State != domain.StateCompleted || retained.SegmentCount() != 1 {
		t.Fatalf("canonical recording was not retained after Watch went offline: %#v, err=%v", retained, err)
	}
	checks, resolves := adapter.counts()
	if checks != 3 || resolves != 0 {
		t.Fatalf("adapter calls = watch checks %d, resolve %d; want 3 checks and no Resolve", checks, resolves)
	}
	if got := statusRequests.Load(); got != 3 {
		t.Fatalf("Owncast status requests = %d, want 3", got)
	}
}

func waitOwncastIntegration(t *testing.T, label string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", label)
		case <-ticker.C:
		}
	}
}

func stringPointer(value string) *string { return &value }

func hasCapability(capabilities []string, want string) bool {
	for _, capability := range capabilities {
		if capability == want {
			return true
		}
	}
	return false
}
