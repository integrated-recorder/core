package acquire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/storage"
)

var errHistoricalScratch = errors.New("historical scratch storage is unavailable")

const (
	maxHistoricalScratchObjects            = maxHistoricalFetchConcurrency
	maxHistoricalScratchBytes              = int64(2 << 30)
	historicalScratchReleaseAttemptTimeout = time.Second
)

const historicalScratchReleaseAttempts = 3

var processHistoricalSpools = newHistoricalSpoolPool(maxHistoricalScratchObjects, maxHistoricalScratchBytes)

type historicalSpoolPool struct {
	mu          sync.Mutex
	limit       int
	byteLimit   int64
	active      int
	activeBytes int64
	changed     chan struct{}
}

func maxConcurrentHistoricalSpools(options storage.IngestOptions) int {
	if options.MaxPayloadBytes <= 0 {
		return 0
	}
	byBytes := int(maxHistoricalScratchBytes / options.MaxPayloadBytes)
	if byBytes < 1 {
		return 0
	}
	return min(maxHistoricalScratchObjects, byBytes)
}

func newHistoricalSpoolPool(limit int, byteLimits ...int64) *historicalSpoolPool {
	if limit < 1 {
		limit = 1
	}
	if limit > maxHistoricalFetchConcurrency {
		limit = maxHistoricalFetchConcurrency
	}
	byteLimit := maxHistoricalScratchBytes
	if len(byteLimits) > 0 && byteLimits[0] > 0 {
		byteLimit = min(byteLimits[0], maxHistoricalScratchBytes)
	}
	return &historicalSpoolPool{limit: limit, byteLimit: byteLimit, changed: make(chan struct{})}
}

func (p *historicalSpoolPool) capacity() int {
	if p == nil || p.limit < 1 {
		return 1
	}
	return p.limit
}

func (p *historicalSpoolPool) capacityFor(maxPayloadBytes int64) int {
	if p == nil || maxPayloadBytes <= 0 || maxPayloadBytes > p.byteLimit {
		return 0
	}
	return min(p.capacity(), int(p.byteLimit/maxPayloadBytes))
}

func (p *historicalSpoolPool) acquire(ctx context.Context, count int, bytes int64) (func(), error) {
	if p == nil {
		return func() {}, nil
	}
	if count < 1 || count > p.limit || bytes <= 0 || bytes > p.byteLimit {
		return nil, errors.New("invalid historical spool reservation")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p.mu.Lock()
		if p.limit-p.active >= count && p.byteLimit-p.activeBytes >= bytes {
			p.active += count
			p.activeBytes += bytes
			p.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					p.mu.Lock()
					p.active -= count
					p.activeBytes -= bytes
					p.signalLocked()
					p.mu.Unlock()
				})
			}, nil
		}
		changed := p.changed
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (m *Manager) historicalWindowCapacity() int {
	if m == nil || m.historicalSpools == nil || m.ingest == nil {
		return 0
	}
	return m.historicalSpools.capacityFor(m.ingest.Options().MaxPayloadBytes)
}

func (m *Manager) acquireHistoricalScratch(ctx context.Context, count int) (func() error, error) {
	if m == nil || m.historicalSpools == nil || m.ingest == nil || count < 1 {
		return nil, errHistoricalScratch
	}
	maxPayloadBytes := m.ingest.Options().MaxPayloadBytes
	if maxPayloadBytes <= 0 || int64(count) > int64(^uint64(0)>>1)/maxPayloadBytes {
		return nil, errHistoricalScratch
	}
	reservationBytes := int64(count) * maxPayloadBytes
	leaseID := ""
	m.mu.RLock()
	coordinator := m.historicalScratchCoordinator
	m.mu.RUnlock()
	if coordinator != nil {
		if err := m.retryPendingHistoricalScratchReleases(ctx, coordinator); err != nil {
			return nil, errors.Join(retryableHistorical(errHistoricalScratch), err)
		}
		var err error
		leaseID, err = newID()
		if err != nil {
			return nil, errors.Join(errHistoricalScratch, err)
		}
	}
	releaseLocal, err := m.historicalSpools.acquire(ctx, count, reservationBytes)
	if err != nil {
		return nil, err
	}
	var once sync.Once
	var releaseErr error
	release := func() error {
		once.Do(func() {
			releaseLocal()
			if coordinator != nil {
				releaseErr = m.releaseHistoricalScratchLease(coordinator, leaseID)
			}
		})
		return releaseErr
	}
	if coordinator != nil {
		if err := coordinator.AcquireHistoricalScratch(ctx, leaseID, count, reservationBytes); err != nil {
			releaseErr := release()
			if ctx.Err() != nil {
				return nil, errors.Join(err, ctx.Err(), releaseErr)
			}
			return nil, errors.Join(errHistoricalScratch, err, releaseErr)
		}
	}
	return release, nil
}

func releaseHistoricalScratchWithRetry(parent context.Context, coordinator HistoricalScratchCoordinator, leaseID string) error {
	if parent == nil {
		parent = context.Background()
	}
	var lastErr error
	for attempt := 0; attempt < historicalScratchReleaseAttempts; attempt++ {
		if err := parent.Err(); err != nil {
			return errors.Join(errHistoricalScratch, err)
		}
		ctx, cancel := context.WithTimeout(parent, historicalScratchReleaseAttemptTimeout)
		lastErr = coordinator.ReleaseHistoricalScratch(ctx, leaseID)
		cancel()
		if lastErr == nil {
			return nil
		}
		if err := parent.Err(); err != nil {
			return errors.Join(errHistoricalScratch, lastErr, err)
		}
		if attempt == historicalScratchReleaseAttempts-1 {
			break
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-parent.Done():
			timer.Stop()
			return errors.Join(errHistoricalScratch, lastErr, parent.Err())
		case <-timer.C:
		}
	}
	return errors.Join(errHistoricalScratch, lastErr)
}

func (m *Manager) releaseHistoricalScratchLease(coordinator HistoricalScratchCoordinator, leaseID string) error {
	m.historicalScratchReleaseMu.Lock()
	defer m.historicalScratchReleaseMu.Unlock()
	err := releaseHistoricalScratchWithRetry(context.Background(), coordinator, leaseID)
	if err != nil {
		if m.pendingHistoricalScratchReleases == nil {
			m.pendingHistoricalScratchReleases = make(map[string]struct{})
		}
		m.pendingHistoricalScratchReleases[leaseID] = struct{}{}
		return err
	}
	delete(m.pendingHistoricalScratchReleases, leaseID)
	return nil
}

func (m *Manager) retryPendingHistoricalScratchReleases(ctx context.Context, coordinator HistoricalScratchCoordinator) error {
	m.historicalScratchReleaseMu.Lock()
	defer m.historicalScratchReleaseMu.Unlock()
	var releaseErrors []error
	for leaseID := range m.pendingHistoricalScratchReleases {
		if err := releaseHistoricalScratchWithRetry(ctx, coordinator, leaseID); err != nil {
			releaseErrors = append(releaseErrors, err)
			continue
		}
		delete(m.pendingHistoricalScratchReleases, leaseID)
	}
	return errors.Join(releaseErrors...)
}

func (p *historicalSpoolPool) signalLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

type historicalPayloadSpool struct {
	file   *os.File
	path   string
	dir    string
	size   int64
	sha256 string
}

func newHistoricalSpoolDirectory() (string, error) {
	directory, err := os.MkdirTemp("", "integrated-recorder-history-")
	if err != nil {
		return "", errHistoricalScratch
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		_ = os.Remove(directory)
		return "", errHistoricalScratch
	}
	return directory, nil
}

func newHistoricalPayloadSpool(directory string) (*historicalPayloadSpool, error) {
	file, err := os.CreateTemp(directory, "payload-*.tmp")
	if err != nil {
		return nil, errors.New("historical scratch file could not be created")
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, errors.New("historical scratch file permissions could not be set")
	}
	spool := &historicalPayloadSpool{file: file, path: file.Name(), dir: directory}
	// Unlinking an open file works on Unix. It prevents stale scratch files if
	// the process exits before normal cleanup. Keep the path where the platform
	// does not support unlinking an open file; Close removes it there.
	if err := os.Remove(spool.path); err == nil {
		spool.path = ""
	}
	return spool, nil
}

func (s *historicalPayloadSpool) write(reader io.Reader, maxBytes, expectedBytes int64) error {
	if s == nil || s.file == nil || reader == nil || maxBytes <= 0 || expectedBytes < -1 {
		return errors.New("invalid historical scratch write")
	}
	hasher := sha256.New()
	limited := io.LimitReader(reader, maxBytes+1)
	writer := &historicalSpoolWriter{file: s.file, hash: hasher, limit: maxBytes}
	n, err := io.Copy(writer, limited)
	if err != nil {
		if errors.Is(err, errHistoricalScratch) {
			return errHistoricalScratch
		}
		if errors.Is(err, storage.ErrIngestTooLarge) {
			return storage.ErrIngestTooLarge
		}
		return err
	}
	if expectedBytes >= 0 && n != expectedBytes {
		return storage.ErrIngestSizeMismatch
	}
	s.size = n
	s.sha256 = hex.EncodeToString(hasher.Sum(nil))
	return nil
}

type historicalSpoolWriter struct {
	file    *os.File
	hash    io.Writer
	limit   int64
	written int64
}

func (w *historicalSpoolWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > w.limit-w.written {
		return 0, storage.ErrIngestTooLarge
	}
	n, err := w.file.Write(data)
	if err != nil {
		return n, errHistoricalScratch
	}
	if n != len(data) {
		return n, errHistoricalScratch
	}
	if _, err := w.hash.Write(data[:n]); err != nil {
		return n, errHistoricalScratch
	}
	w.written += int64(n)
	return n, nil
}

func (s *historicalPayloadSpool) readPayload(ctx context.Context, ingest *storage.IngestService, recordingID string) (*storage.IngestPayload, error) {
	if s == nil || s.file == nil || ingest == nil || s.size < 0 || s.sha256 == "" {
		return nil, errors.New("invalid historical scratch payload")
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, errHistoricalScratch
	}
	options := ingest.Options()
	payload, err := ingest.ReadPayload(ctx, recordingID, s.file, options.MaxPayloadBytes, s.size, s.size)
	if err != nil {
		return nil, err
	}
	result := payload.Result()
	if result.Size != s.size || result.SHA256 != s.sha256 {
		payload.Release()
		return nil, errHistoricalScratch
	}
	return payload, nil
}

func (s *historicalPayloadSpool) close() error {
	if s == nil || s.file == nil {
		return nil
	}
	file := s.file
	s.file = nil
	closeErr := file.Close()
	if s.path != "" {
		if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) && closeErr == nil {
			closeErr = errHistoricalScratch
		}
	}
	if closeErr != nil {
		return errHistoricalScratch
	}
	return nil
}

func removeHistoricalSpoolDirectory(directory string) error {
	if directory == "" {
		return nil
	}
	if err := os.Remove(directory); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: directory cleanup", errHistoricalScratch)
	}
	return nil
}

type historicalSpoolFetchResult struct {
	spool *historicalPayloadSpool
	err   error
}

func (m *Manager) fetchHistoricalWindow(ctx context.Context, recordingID string, media adapterproto.MediaSource, e *entry, generation uint64, candidates []historicalRecoveryWork) ([]historicalSpoolFetchResult, func() error, error) {
	if len(candidates) == 0 {
		return nil, func() error { return nil }, nil
	}
	if m.historicalSpools == nil {
		return nil, nil, retryableHistorical(errHistoricalScratch)
	}
	releaseSlots, err := m.acquireHistoricalScratch(ctx, len(candidates))
	if err != nil {
		return nil, nil, err
	}
	directory, err := newHistoricalSpoolDirectory()
	if err != nil {
		return nil, nil, errors.Join(retryableHistorical(errHistoricalScratch), releaseSlots())
	}
	spools := make([]*historicalPayloadSpool, 0, len(candidates))
	var cleanupOnce sync.Once
	var cleanupErr error
	cleanup := func() error {
		cleanupOnce.Do(func() {
			for _, spool := range spools {
				if err := spool.close(); err != nil && cleanupErr == nil {
					cleanupErr = errHistoricalScratch
				}
			}
			if err := removeHistoricalSpoolDirectory(directory); err != nil && cleanupErr == nil {
				cleanupErr = errHistoricalScratch
			}
			if err := releaseSlots(); err != nil {
				cleanupErr = errors.Join(cleanupErr, err)
			}
		})
		return cleanupErr
	}
	for range candidates {
		spool, createErr := newHistoricalPayloadSpool(directory)
		if createErr != nil {
			return nil, nil, errors.Join(retryableHistorical(errHistoricalScratch), cleanup())
		}
		spools = append(spools, spool)
	}
	type completion struct {
		index int
		err   error
	}
	completed := make(chan completion, len(candidates))
	for index, candidate := range candidates {
		spool := spools[index]
		go func(index int, candidate historicalRecoveryWork, spool *historicalPayloadSpool) {
			completed <- completion{index: index, err: m.downloadHistoricalToSpool(ctx, candidate.source, media, e, generation, spool)}
		}(index, candidate, spool)
	}
	results := make([]historicalSpoolFetchResult, len(candidates))
	for range candidates {
		result := <-completed
		results[result.index] = historicalSpoolFetchResult{spool: spools[result.index], err: result.err}
	}
	return results, cleanup, nil
}
