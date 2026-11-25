package replication

import (
	"context"
	"errors"
	"sync"
)

var ErrImportGateKey = errors.New("replica import gate key is invalid")

type importGateEntry struct {
	semaphore chan struct{}
	users     int
}

// ImportGate serializes foreground and background work for the same durable
// lease. It is process-local coordination only; lease and filesystem locks
// remain the cross-process durability boundaries.
type ImportGate struct {
	mu             sync.Mutex
	entries        map[string]*importGateEntry
	sourceVerified map[string]string
}

func NewImportGate() *ImportGate {
	return &ImportGate{entries: make(map[string]*importGateEntry), sourceVerified: make(map[string]string)}
}

// MarkSourceVerified records only an in-process optimization after the full
// import/readback API has returned success. It is intentionally not durable:
// a new Control Plane rechecks active leases once because the peer response
// may have been lost after the local lease became active.
func (g *ImportGate) MarkSourceVerified(leaseID, offerHash string) {
	if g == nil || leaseID == "" || offerHash == "" {
		return
	}
	g.mu.Lock()
	if g.sourceVerified == nil {
		g.sourceVerified = make(map[string]string)
	}
	g.sourceVerified[leaseID] = offerHash
	g.mu.Unlock()
}

func (g *ImportGate) SourceVerified(leaseID, offerHash string) bool {
	if g == nil || leaseID == "" || offerHash == "" {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.sourceVerified[leaseID] == offerHash
}

// RetainSourceVerified bounds the process-local cache to leases that remain
// active with the exact immutable offer hash observed in the durable store.
func (g *ImportGate) RetainSourceVerified(activeOffers map[string]string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for leaseID, offerHash := range g.sourceVerified {
		if activeOffers[leaseID] != offerHash {
			delete(g.sourceVerified, leaseID)
		}
	}
}

// Acquire returns a release function for one lease-scoped import. Waiters can
// abandon their wait through ctx; different leases do not block each other.
func (g *ImportGate) Acquire(ctx context.Context, leaseID string) (func(), error) {
	if g == nil || ctx == nil || leaseID == "" || len(leaseID) > 128 {
		return nil, ErrImportGateKey
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	if g.entries == nil {
		g.entries = make(map[string]*importGateEntry)
	}
	entry := g.entries[leaseID]
	if entry == nil {
		entry = &importGateEntry{semaphore: make(chan struct{}, 1)}
		entry.semaphore <- struct{}{}
		g.entries[leaseID] = entry
	}
	entry.users++
	g.mu.Unlock()

	select {
	case <-ctx.Done():
		g.dropUser(leaseID, entry)
		return nil, ctx.Err()
	case <-entry.semaphore:
	}
	// If cancellation raced with a ready semaphore, select may choose either
	// case. Do not admit work after the caller's context has already expired.
	if err := ctx.Err(); err != nil {
		entry.semaphore <- struct{}{}
		g.dropUser(leaseID, entry)
		return nil, err
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			entry.semaphore <- struct{}{}
			g.dropUser(leaseID, entry)
		})
	}, nil
}

func (g *ImportGate) dropUser(leaseID string, entry *importGateEntry) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if entry.users > 0 {
		entry.users--
	}
	if entry.users == 0 && g.entries[leaseID] == entry {
		delete(g.entries, leaseID)
	}
}
