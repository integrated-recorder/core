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

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/storage"
)

var errHistoricalScratch = errors.New("historical scratch storage is unavailable")

const maxHistoricalScratchObjects = maxHistoricalFetchConcurrency

type historicalSpoolPool struct {
	mu      sync.Mutex
	limit   int
	active  int
	changed chan struct{}
}

func maxConcurrentHistoricalSpools(_ storage.IngestOptions) int {
	// Spools live on private scratch storage. Bound them independently from
	// ingest RAM: each response still has MaxPayloadBytes cap, so this pool
	// bounds aggregate scratch use to maxHistoricalScratchObjects payloads.
	return maxHistoricalScratchObjects
}

func newHistoricalSpoolPool(limit int) *historicalSpoolPool {
	if limit < 1 {
		limit = 1
	}
	if limit > maxHistoricalFetchConcurrency {
		limit = maxHistoricalFetchConcurrency
	}
	return &historicalSpoolPool{limit: limit, changed: make(chan struct{})}
}

func (p *historicalSpoolPool) capacity() int {
	if p == nil || p.limit < 1 {
		return 1
	}
	return p.limit
}

func (p *historicalSpoolPool) acquire(ctx context.Context, count int) (func(), error) {
	if p == nil {
		return func() {}, nil
	}
	if count < 1 || count > p.limit {
		return nil, errors.New("invalid historical spool reservation")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p.mu.Lock()
		if p.limit-p.active >= count {
			p.active += count
			p.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					p.mu.Lock()
					p.active -= count
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
	releaseSlots, err := m.historicalSpools.acquire(ctx, len(candidates))
	if err != nil {
		return nil, nil, err
	}
	directory, err := newHistoricalSpoolDirectory()
	if err != nil {
		releaseSlots()
		return nil, nil, retryableHistorical(errHistoricalScratch)
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
			releaseSlots()
		})
		return cleanupErr
	}
	for range candidates {
		spool, createErr := newHistoricalPayloadSpool(directory)
		if createErr != nil {
			_ = cleanup()
			return nil, nil, retryableHistorical(errHistoricalScratch)
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
