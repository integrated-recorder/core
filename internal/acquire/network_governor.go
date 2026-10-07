package acquire

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/storage"
)

const (
	minHistoricalFetchConcurrency     = 1
	initialHistoricalFetchConcurrency = 2
	maxHistoricalFetchConcurrency     = 4
	historicalHealthyInterval         = 10 * time.Second
	historicalAdmissionPoll           = 100 * time.Millisecond
)

type acquisitionPriority uint8

const (
	acquisitionPriorityLive acquisitionPriority = iota
	acquisitionPriorityHistorical
)

type acquisitionPriorityKey struct{}

func withHistoricalAcquisitionPriority(ctx context.Context) context.Context {
	return context.WithValue(ctx, acquisitionPriorityKey{}, acquisitionPriorityHistorical)
}

func requestAcquisitionPriority(ctx context.Context) acquisitionPriority {
	priority, _ := ctx.Value(acquisitionPriorityKey{}).(acquisitionPriority)
	return priority
}

type historicalPressure uint8

const (
	historicalPressureHealthy historicalPressure = iota
	historicalPressureModerate
	historicalPressureSevere
)

type historicalFetchGovernor struct {
	mu           sync.Mutex
	limit        int
	active       int
	liveActive   int
	healthySince time.Time
	changed      chan struct{}
	snapshot     func() storage.IngestSnapshot
	clock        func() time.Time
}

func newHistoricalFetchGovernor(snapshot func() storage.IngestSnapshot) *historicalFetchGovernor {
	return &historicalFetchGovernor{
		limit:    initialHistoricalFetchConcurrency,
		changed:  make(chan struct{}),
		snapshot: snapshot,
		clock:    time.Now,
	}
}

func (g *historicalFetchGovernor) currentLimit() int {
	if g == nil {
		return minHistoricalFetchConcurrency
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.limit
}

func (g *historicalFetchGovernor) waitWindow(ctx context.Context, maximum int) (int, error) {
	if maximum < minHistoricalFetchConcurrency {
		maximum = minHistoricalFetchConcurrency
	}
	if g == nil {
		return min(maximum, initialHistoricalFetchConcurrency), nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		g.observe(g.currentSnapshot(), g.now())
		g.mu.Lock()
		limit := g.limit
		changed := g.changed
		g.mu.Unlock()
		if limit > 0 {
			return min(limit, maximum), nil
		}
		timer := time.NewTimer(historicalAdmissionPoll)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return 0, ctx.Err()
		case <-changed:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
}

func (g *historicalFetchGovernor) now() time.Time {
	g.mu.Lock()
	clock := g.clock
	g.mu.Unlock()
	if clock == nil {
		return time.Now()
	}
	return clock()
}

func (g *historicalFetchGovernor) currentSnapshot() storage.IngestSnapshot {
	if g == nil || g.snapshot == nil {
		return storage.IngestSnapshot{}
	}
	return g.snapshot()
}

func (g *historicalFetchGovernor) observe(snapshot storage.IngestSnapshot, now time.Time) {
	if g == nil {
		return
	}
	pressure := classifyHistoricalPressure(snapshot)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.liveActive > 0 {
		pressure = historicalPressureSevere
	}
	switch pressure {
	case historicalPressureSevere:
		g.setLimitLocked(0)
		g.healthySince = time.Time{}
	case historicalPressureModerate:
		g.setLimitLocked(minHistoricalFetchConcurrency)
		g.healthySince = time.Time{}
	default:
		if g.limit == 0 {
			g.setLimitLocked(minHistoricalFetchConcurrency)
		}
		if g.healthySince.IsZero() {
			g.healthySince = now
		} else if now.Sub(g.healthySince) >= historicalHealthyInterval && g.limit < maxHistoricalFetchConcurrency {
			g.setLimitLocked(g.limit + 1)
			g.healthySince = now
		}
	}
}

func (g *historicalFetchGovernor) setLimitLocked(limit int) {
	if g.limit == limit {
		return
	}
	g.limit = limit
	g.signalLocked()
}

func classifyHistoricalPressure(snapshot storage.IngestSnapshot) historicalPressure {
	capacity := snapshot.BufferCapacityBytes
	used := snapshot.BufferUsedBytes + snapshot.ReservedBytes
	if capacity > 0 {
		if used*100 >= capacity*90 || snapshot.QueueBytes*100 >= capacity*70 ||
			snapshot.QueueObjects >= storage.MaxIngestObjects*3/4 || snapshot.OldestPersistAgeSeconds >= 5 {
			return historicalPressureSevere
		}
	}
	if capacity > 0 && (used*100 >= capacity*65 || snapshot.QueueBytes*100 >= capacity*35) {
		return historicalPressureModerate
	}
	if snapshot.QueueObjects >= storage.MaxIngestObjects/4 || snapshot.OldestPersistAgeSeconds >= 1 {
		return historicalPressureModerate
	}
	if snapshot.WriterConcurrency > 0 && snapshot.ActiveWriters >= snapshot.WriterConcurrency {
		return historicalPressureModerate
	}
	return historicalPressureHealthy
}

func (g *historicalFetchGovernor) acquire(ctx context.Context) (func(), error) {
	if g == nil {
		return func() {}, nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		now := g.now()
		g.observe(g.currentSnapshot(), now)
		g.mu.Lock()
		if g.limit > 0 && g.active < g.limit {
			g.active++
			g.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					g.mu.Lock()
					if g.active > 0 {
						g.active--
					}
					g.signalLocked()
					g.mu.Unlock()
				})
			}, nil
		}
		changed := g.changed
		g.mu.Unlock()
		timer := time.NewTimer(historicalAdmissionPoll)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-changed:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
}

func (g *historicalFetchGovernor) acquireLive() func() {
	if g == nil {
		return func() {}
	}
	g.mu.Lock()
	wasIdle := g.liveActive == 0
	g.liveActive++
	if wasIdle {
		g.signalLocked()
	}
	g.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			if g.liveActive > 0 {
				g.liveActive--
			}
			if g.liveActive == 0 {
				g.signalLocked()
			}
			g.mu.Unlock()
		})
	}
}

func (g *historicalFetchGovernor) signalLocked() {
	close(g.changed)
	g.changed = make(chan struct{})
}

type historicalPriorityRoundTripper struct {
	base     http.RoundTripper
	governor *historicalFetchGovernor
}

func (t historicalPriorityRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if requestAcquisitionPriority(request.Context()) != acquisitionPriorityHistorical {
		release := t.governor.acquireLive()
		response, err := t.base.RoundTrip(request)
		if err != nil {
			release()
			return nil, err
		}
		if response.Body == nil {
			release()
			return response, nil
		}
		response.Body = &governedResponseBody{ReadCloser: response.Body, release: release}
		return response, nil
	}
	release, err := t.governor.acquire(request.Context())
	if err != nil {
		return nil, err
	}
	response, err := t.base.RoundTrip(request)
	if err != nil {
		release()
		return nil, err
	}
	if response.Body == nil {
		release()
		return response, nil
	}
	response.Body = &governedResponseBody{ReadCloser: response.Body, release: release}
	return response, nil
}

type governedResponseBody struct {
	io.ReadCloser
	release func()
	once    sync.Once
}

func (b *governedResponseBody) Read(buffer []byte) (int, error) {
	n, err := b.ReadCloser.Read(buffer)
	if err != nil {
		b.releaseOnce()
	}
	return n, err
}

func (b *governedResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.releaseOnce()
	return err
}

func (b *governedResponseBody) releaseOnce() {
	b.once.Do(b.release)
}
