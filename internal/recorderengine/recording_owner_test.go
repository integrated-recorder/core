package recorderengine_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/storage"
)

type recordingOwnerFixtureClient struct {
	mu            sync.Mutex
	store         *recordingowner.Store
	archive       *storage.Store
	ids           []string
	claims        []recordingowner.Owner
	releases      []recordingowner.Owner
	generatedID   string
	claimObserved bool
}

func (f *recordingOwnerFixtureClient) snapshot() (ids []string, claims, releases []recordingowner.Owner, claimObserved bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ids...), append([]recordingowner.Owner(nil), f.claims...), append([]recordingowner.Owner(nil), f.releases...), f.claimObserved
}

func (f *recordingOwnerFixtureClient) ClaimRecording(_ context.Context, requested string) (recordingowner.Owner, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := requested
	if id == "" {
		id = f.generatedID
	}
	if _, err := f.archive.LoadRecordingReadOnly(id); !errors.Is(err, storage.ErrNotFound) {
		return recordingowner.Owner{}, errors.New("archive existed before Host Claim")
	}
	f.claimObserved = true
	owner, err := f.store.Claim(id, fixtureGenerationID, fixtureWorkerInstanceID)
	if err != nil {
		return recordingowner.Owner{}, err
	}
	f.ids = append(f.ids, requested)
	f.claims = append(f.claims, owner)
	return owner, nil
}

func (f *recordingOwnerFixtureClient) ReleaseRecording(_ context.Context, owner recordingowner.Owner) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, existing := range f.releases {
		if existing == owner {
			return nil
		}
	}
	if err := f.store.Release(owner); err != nil {
		return err
	}
	f.releases = append(f.releases, owner)
	return nil
}

const (
	fixtureGenerationID     = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	fixtureWorkerInstanceID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	generatedRecordingID    = "cccccccccccccccccccccccccccccccc"
)

func newManagedEngineFixture(t *testing.T, transport http.RoundTripper) (*recorderengine.Engine, *acquire.Manager, *recordingOwnerFixtureClient, *storage.Store) {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "data")
	archive, err := storage.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	ownerStore, err := recordingowner.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManagerWithMode(archive, &http.Client{Transport: transport, Timeout: time.Second}, nil, func(context.Context, string) error { return nil }, acquire.FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ConfigureCanonicalCommitFence(ownerStore); err != nil {
		t.Fatal(err)
	}
	engine, err := recorderengine.New(manager, &adapterhost.Host{}, fixtureGenerationID, fixtureWorkerInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	ownerClient := &recordingOwnerFixtureClient{store: ownerStore, archive: archive, generatedID: generatedRecordingID}
	if err := engine.ConfigureRecordingOwnerClient(ownerClient); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := engine.Close(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("close managed engine: %v", err)
		}
	})
	return engine, manager, ownerClient, archive
}

func managedStartPayload(t *testing.T, id string, media adapterproto.MediaSource) []byte {
	t.Helper()
	payload, err := json.Marshal(recorderengine.StartResolvedRequest{
		RecordingID: id, AdapterID: "fixture", Media: media, Title: "owned recording",
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestManagedEngineClaimsBeforeCanonicalCreateAndUsesHostID(t *testing.T) {
	engine, _, ownerClient, archive := newManagedEngineFixture(t, fixtureTransport{})
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: "https://fixture.example/live.m3u8"}
	result, err := engine.Handle(context.Background(), recorderengine.OperationStartResolved, managedStartPayload(t, "", media))
	if err != nil {
		t.Fatal(err)
	}
	started, ok := result.(*domain.Recording)
	if !ok || started.ID != generatedRecordingID || started.State != domain.StateRecording {
		t.Fatalf("managed start result = %#v", result)
	}
	_, claims, _, observed := ownerClient.snapshot()
	if !observed || len(claims) != 1 || claims[0].RecordingID != generatedRecordingID {
		t.Fatalf("Host claim ordering/tuple was not preserved: observed=%v claims=%+v", observed, claims)
	}
	if _, err := archive.LoadRecordingReadOnly(generatedRecordingID); err != nil {
		t.Fatalf("canonical recording was not created after Host Claim: %v", err)
	}
	if _, err := engine.Handle(context.Background(), recorderengine.OperationStop, mustJSON(t, recorderengine.RecordingIDRequest{RecordingID: generatedRecordingID})); err != nil {
		t.Fatalf("managed stop: %v", err)
	}
	_, claims, releases, _ := ownerClient.snapshot()
	if len(releases) != 1 || releases[0] != claims[0] {
		t.Fatalf("Host release tuple = %+v; claim = %+v", releases, claims)
	}
}

func TestManagedStopRetainsOwnerUntilAutomaticHistoricalRepairFinishes(t *testing.T) {
	transport := &blockingTerminalRecoveryTransport{historyStarted: make(chan struct{}), resumeHistory: make(chan struct{})}
	engine, manager, ownerClient, _ := newManagedEngineFixture(t, transport)
	media := adapterproto.MediaSource{
		Type: "hls", ManifestURL: "https://fixture.example/live.m3u8",
		HistoricalAvailability: &adapterproto.HistoricalAvailability{
			Mode:                  adapterproto.HistoricalModeSequenceRanges,
			SequenceRanges:        []adapterproto.HistoricalSequenceRange{{Start: 2, End: 2}},
			HistoricalManifestURL: "https://fixture.example/history.m3u8",
		},
	}
	if _, err := engine.Handle(context.Background(), recorderengine.OperationStartResolved, managedStartPayload(t, generatedRecordingID, media)); err != nil {
		t.Fatalf("managed start: %v", err)
	}
	select {
	case <-transport.historyStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("automatic historical recovery did not start")
	}
	stopped, err := engine.Handle(context.Background(), recorderengine.OperationStop, mustJSON(t, recorderengine.RecordingIDRequest{RecordingID: generatedRecordingID}))
	if err != nil || stopped.(*domain.Recording).State == domain.StateRecording {
		t.Fatalf("managed stop result=%#v err=%v", stopped, err)
	}
	_, claims, releases, _ := ownerClient.snapshot()
	if len(claims) != 1 || len(releases) != 0 {
		t.Fatalf("Stop released generation owner before historical recovery finished: claims=%+v releases=%+v", claims, releases)
	}
	current, err := ownerClient.store.Current(generatedRecordingID)
	if err != nil || current != claims[0] {
		t.Fatalf("terminal repair lost its generation owner lease: owner=%+v err=%v", current, err)
	}

	close(transport.resumeHistory)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		current, getErr := manager.Get(generatedRecordingID)
		_, _, releases, _ = ownerClient.snapshot()
		if getErr == nil && hasRecordingSequence(current, 2) && len(releases) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, _, releases, _ = ownerClient.snapshot()
	recording, getErr := manager.Get(generatedRecordingID)
	if len(releases) != 1 || releases[0] != claims[0] || getErr != nil || !hasRecordingSequence(recording, 2) {
		t.Fatalf("terminal recovery did not commit history and reach idle: releases=%+v history_requests=%d recording=%#v get_error=%v", releases, transport.historyRequests.Load(), recording, getErr)
	}
}

func TestManagedEngineHonorsExplicitIDAndReleasesFailedStart(t *testing.T) {
	engine, _, ownerClient, archive := newManagedEngineFixture(t, fixtureTransport{})
	const explicitID = "dddddddddddddddddddddddddddddddd"
	invalidMedia := adapterproto.MediaSource{Type: "unsupported", ManifestURL: "https://fixture.example/live.m3u8"}
	if _, err := engine.Handle(context.Background(), recorderengine.OperationStartResolved, managedStartPayload(t, explicitID, invalidMedia)); err == nil {
		t.Fatal("invalid media start unexpectedly succeeded")
	}
	ids, claims, releases, _ := ownerClient.snapshot()
	if len(claims) != 1 || ids[0] != explicitID || claims[0].RecordingID != explicitID {
		t.Fatalf("explicit Host Claim mismatch: ids=%v claims=%+v", ids, claims)
	}
	if len(releases) != 1 || releases[0] != claims[0] {
		t.Fatalf("failed start did not release exact Host claim: releases=%+v claims=%+v", releases, claims)
	}
	if _, err := archive.LoadRecordingReadOnly(explicitID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("failed start created canonical archive: %v", err)
	}
	if _, err := ownerClient.store.Current(explicitID); !errors.Is(err, recordingowner.ErrNotFound) {
		t.Fatalf("failed start left owner record: %v", err)
	}
}

func TestManagedEngineNaturalCompletionReleasesAfterDurableTerminalState(t *testing.T) {
	engine, manager, ownerClient, archive := newManagedEngineFixture(t, endListFixtureTransport{})
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: "https://fixture.example/live.m3u8"}
	result, err := engine.Handle(context.Background(), recorderengine.OperationStartResolved, managedStartPayload(t, generatedRecordingID, media))
	if err != nil {
		t.Fatal(err)
	}
	started := result.(*domain.Recording)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, _, releases, _ := ownerClient.snapshot()
		if len(releases) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, _, releases, _ := ownerClient.snapshot()
	if len(releases) != 1 {
		t.Fatalf("natural completion did not release owner: %+v", releases)
	}
	terminal, err := manager.Get(started.ID)
	if err != nil || terminal.State != domain.StateCompleted {
		t.Fatalf("terminal state=%q err=%v", stateOf(terminal), err)
	}
	if _, err := archive.LoadRecordingReadOnly(started.ID); err != nil {
		t.Fatalf("terminal archive is unavailable: %v", err)
	}
	if _, err := ownerClient.store.Current(started.ID); !errors.Is(err, recordingowner.ErrNotFound) {
		t.Fatalf("owner remained after natural completion: %v", err)
	}
}

func TestDirectEngineWithoutRuntimeOwnerClientRemainsCompatible(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManagerWithMode(store, &http.Client{Transport: fixtureTransport{}, Timeout: time.Second}, nil, func(context.Context, string) error { return nil }, acquire.FreshGeneration)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := recorderengine.New(manager, &adapterhost.Host{}, fixtureGenerationID, fixtureWorkerInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close(context.Background())
	const id = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: "https://fixture.example/live.m3u8"}
	result, err := engine.Handle(context.Background(), recorderengine.OperationStartResolved, managedStartPayload(t, id, media))
	if err != nil {
		t.Fatal(err)
	}
	if result.(*domain.Recording).ID != id {
		t.Fatalf("direct Engine did not preserve explicit ID: %#v", result)
	}
}

type endListFixtureTransport struct{}

func (endListFixtureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	body := ""
	switch request.URL.Path {
	case "/live.m3u8":
		body = "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:1,fixture\nsegment.ts\n#EXT-X-ENDLIST\n"
	case "/segment.ts":
		body = "fixture media bytes"
	default:
		return nil, io.EOF
	}
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: request}, nil
}

type blockingTerminalRecoveryTransport struct {
	historyStarted  chan struct{}
	resumeHistory   chan struct{}
	historyRequests atomic.Int32
	once            sync.Once
}

func (t *blockingTerminalRecoveryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var body string
	switch request.URL.Path {
	case "/live.m3u8":
		body = "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n#EXTINF:1,fixture\nsegment.ts\n"
	case "/history.m3u8":
		t.historyRequests.Add(1)
		t.once.Do(func() { close(t.historyStarted) })
		select {
		case <-t.resumeHistory:
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
		body = "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:2\n#EXTINF:1,fixture\nhistory-segment.ts\n#EXT-X-ENDLIST\n"
	case "/segment.ts":
		body = "fixture media bytes"
	case "/history-segment.ts":
		body = "historical bytes committed after capture stopped"
	default:
		return nil, io.EOF
	}
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)), Request: request}, nil
}

func hasRecordingSequence(recording *domain.Recording, sequence uint64) bool {
	if recording == nil || recording.Tracks["main"] == nil {
		return false
	}
	for _, segment := range recording.Tracks["main"].Segments {
		if segment.Sequence == sequence {
			return true
		}
	}
	return false
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func stateOf(recording *domain.Recording) domain.RecordingState {
	if recording == nil {
		return "<nil>"
	}
	return recording.State
}
