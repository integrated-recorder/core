package acquire

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/storage"
)

func TestHistoricalFetchGovernorAdaptiveBounds(t *testing.T) {
	governor := newHistoricalFetchGovernor(nil)
	base := time.Unix(1_800_000_000, 0)
	healthy := storage.IngestSnapshot{}
	governor.observe(healthy, base)
	if got := governor.currentLimit(); got != initialHistoricalFetchConcurrency {
		t.Fatalf("initial historical limit=%d, want %d", got, initialHistoricalFetchConcurrency)
	}
	governor.observe(healthy, base.Add(historicalHealthyInterval-time.Second))
	if got := governor.currentLimit(); got != initialHistoricalFetchConcurrency {
		t.Fatalf("limit grew before healthy interval: %d", got)
	}
	governor.observe(healthy, base.Add(historicalHealthyInterval))
	if got := governor.currentLimit(); got != initialHistoricalFetchConcurrency+1 {
		t.Fatalf("first healthy increase=%d, want %d", got, initialHistoricalFetchConcurrency+1)
	}
	governor.observe(healthy, base.Add(2*historicalHealthyInterval))
	if got := governor.currentLimit(); got != maxHistoricalFetchConcurrency {
		t.Fatalf("healthy limit=%d, want cap %d", got, maxHistoricalFetchConcurrency)
	}

	moderate := storage.IngestSnapshot{BufferCapacityBytes: 100, BufferUsedBytes: 65}
	governor.observe(moderate, base.Add(2*historicalHealthyInterval+time.Second))
	if got := governor.currentLimit(); got != minHistoricalFetchConcurrency {
		t.Fatalf("moderate pressure limit=%d, want %d", got, minHistoricalFetchConcurrency)
	}
	severe := storage.IngestSnapshot{BufferCapacityBytes: 100, BufferUsedBytes: 90}
	governor.observe(severe, base.Add(2*historicalHealthyInterval+2*time.Second))
	if got := governor.currentLimit(); got != 0 {
		t.Fatalf("severe pressure limit=%d, want pause", got)
	}
	governor.observe(healthy, base.Add(2*historicalHealthyInterval+3*time.Second))
	if got := governor.currentLimit(); got != minHistoricalFetchConcurrency {
		t.Fatalf("recovery from pause limit=%d, want minimum %d", got, minHistoricalFetchConcurrency)
	}
}

func TestHistoricalFetchGovernorBoundsConcurrentAdmission(t *testing.T) {
	governor := newHistoricalFetchGovernor(nil)
	first, err := governor.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := governor.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	third := make(chan func(), 1)
	governorErr := make(chan error, 1)
	acquireThird := func() {
		release, acquireErr := governor.acquire(context.Background())
		if acquireErr != nil {
			governorErr <- acquireErr
			return
		}
		third <- release
	}
	go acquireThird()
	select {
	case <-third:
		t.Fatal("third historical request exceeded initial concurrency limit")
	case err := <-governorErr:
		t.Fatal(err)
	case <-time.After(25 * time.Millisecond):
	}
	first()
	var releaseThird func()
	select {
	case releaseThird = <-third:
	case err := <-governorErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("historical request did not proceed after a slot was released")
	}
	second()
	releaseThird()
}

func TestHistoricalHTTPAdmissionLetsLiveProceedAndPausesNewHistory(t *testing.T) {
	arrivals := make(chan string, 8)
	base := testRoundTripper(func(request *http.Request) (*http.Response, error) {
		arrivals <- request.URL.Path
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader("held")), Request: request,
		}, nil
	})
	governor := newHistoricalFetchGovernor(nil)
	transport := historicalPriorityRoundTripper{base: base, governor: governor}
	newRequest := func(path string, historical bool) *http.Request {
		ctx := context.Background()
		if historical {
			ctx = withHistoricalAcquisitionPriority(ctx)
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://media.example"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		return request
	}

	firstHistory, err := transport.RoundTrip(newRequest("/history-1", true))
	if err != nil {
		t.Fatal(err)
	}
	if got := <-arrivals; got != "/history-1" {
		t.Fatalf("first arrival=%q", got)
	}
	live, err := transport.RoundTrip(newRequest("/live", false))
	if err != nil {
		t.Fatal(err)
	}
	if got := <-arrivals; got != "/live" {
		t.Fatalf("live request did not pass historical backlog: arrival=%q", got)
	}

	secondHistoryDone := make(chan *http.Response, 1)
	secondHistoryErr := make(chan error, 1)
	go func() {
		response, roundTripErr := transport.RoundTrip(newRequest("/history-2", true))
		if roundTripErr != nil {
			secondHistoryErr <- roundTripErr
			return
		}
		secondHistoryDone <- response
	}()
	select {
	case got := <-arrivals:
		t.Fatalf("new historical request entered while live request remained active: %q", got)
	case err := <-secondHistoryErr:
		t.Fatal(err)
	case <-time.After(30 * time.Millisecond):
	}
	if err := firstHistory.Body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-arrivals:
		t.Fatalf("history resumed before live response closed: %q", got)
	case <-time.After(30 * time.Millisecond):
	}
	if err := live.Body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-secondHistoryDone:
		if err := response.Body.Close(); err != nil {
			t.Fatal(err)
		}
	case err := <-secondHistoryErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("historical admission did not resume after live response closed")
	}
	if got := <-arrivals; got != "/history-2" {
		t.Fatalf("resumed arrival=%q", got)
	}
}

func TestHistoricalSpoolLimitUsesScratchByteBound(t *testing.T) {
	tests := []struct {
		name    string
		options storage.IngestOptions
		want    int
	}{
		{name: "default", options: storage.DefaultIngestOptions(), want: maxHistoricalFetchConcurrency},
		{name: "small ingest pool", options: storage.IngestOptions{GlobalBytes: 20, MaxPayloadBytes: 10}, want: maxHistoricalFetchConcurrency},
		{name: "large ingest pool", options: storage.IngestOptions{GlobalBytes: 80, MaxPayloadBytes: 20}, want: maxHistoricalFetchConcurrency},
		{name: "one gibibyte payload", options: storage.IngestOptions{GlobalBytes: 2 << 30, MaxPayloadBytes: 1 << 30}, want: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := maxConcurrentHistoricalSpools(test.options); got != test.want {
				t.Fatalf("spool limit=%d, want %d", got, test.want)
			}
		})
	}
}

func TestHistoricalPressureDoesNotDoubleCountPayloadBackingMemory(t *testing.T) {
	const capacity = int64(1 << 30)
	tests := []struct {
		name     string
		snapshot storage.IngestSnapshot
		want     historicalPressure
	}{
		{
			name: "partially filled reserved payload",
			snapshot: storage.IngestSnapshot{
				BufferCapacityBytes: capacity, BufferUsedBytes: capacity / 8, ReservedBytes: capacity / 4,
			},
			want: historicalPressureHealthy,
		},
		{
			name: "normal full payload backing",
			snapshot: storage.IngestSnapshot{
				BufferCapacityBytes: capacity, BufferUsedBytes: capacity * 3 / 8, ReservedBytes: capacity / 2,
			},
			want: historicalPressureHealthy,
		},
		{
			name: "multiple payload backing",
			snapshot: storage.IngestSnapshot{
				BufferCapacityBytes: capacity, BufferUsedBytes: capacity * 5 / 8, ReservedBytes: capacity * 3 / 4,
			},
			want: historicalPressureModerate,
		},
		{
			name: "reserved but unused capacity",
			snapshot: storage.IngestSnapshot{
				BufferCapacityBytes: capacity, ReservedBytes: capacity * 95 / 100,
			},
			want: historicalPressureSevere,
		},
		{
			name: "queue bytes remain separate pressure",
			snapshot: storage.IngestSnapshot{
				BufferCapacityBytes: capacity, BufferUsedBytes: capacity / 8,
				ReservedBytes: capacity / 4, QueueBytes: capacity * 3 / 4,
			},
			want: historicalPressureSevere,
		},
		{
			name: "queue objects and age remain separate pressure",
			snapshot: storage.IngestSnapshot{
				BufferCapacityBytes: capacity, BufferUsedBytes: capacity / 8,
				ReservedBytes: capacity / 4, QueueObjects: storage.MaxIngestObjects / 4,
			},
			want: historicalPressureModerate,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyHistoricalPressure(test.snapshot); got != test.want {
				t.Fatalf("pressure=%v, want %v; snapshot=%+v", got, test.want, test.snapshot)
			}
		})
	}
}

func TestHistoricalFetchGovernorSignalsOnlyOnLimitTransitions(t *testing.T) {
	governor := newHistoricalFetchGovernor(nil)
	governor.mu.Lock()
	initial := governor.changed
	governor.mu.Unlock()
	severe := storage.IngestSnapshot{BufferCapacityBytes: 100, BufferUsedBytes: 95}
	governor.observe(severe, time.Now())
	governor.mu.Lock()
	paused := governor.changed
	governor.mu.Unlock()
	if paused == initial {
		t.Fatal("limit transition did not signal admission waiters")
	}
	governor.observe(severe, time.Now().Add(time.Second))
	governor.mu.Lock()
	defer governor.mu.Unlock()
	if governor.changed != paused {
		t.Fatal("repeated same-pressure observation signaled waiters again")
	}
}
