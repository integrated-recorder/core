// Package storageprocess starts a generation-pinned storage provider executable
// and exposes its streamed object protocol as Core's physical object store.
// Archive/domain semantics remain in internal/storage.
package storageprocess

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/runtimehost/storagecatalog"
	"github.com/integrated-recorder/core/internal/storage"
	"github.com/integrated-recorder/core/internal/storageproto"
)

const (
	callTimeout              = 5 * time.Minute
	initialRestartBackoff    = 100 * time.Millisecond
	maximumRestartBackoff    = 5 * time.Second
	exitObservationWindow    = 100 * time.Millisecond
	maxConcurrentReadStreams = storageproto.MaxConcurrentReadStreams
)

// Runtime owns one generation-pinned provider child and the private IPC
// material used to reach it. When a child exits, Runtime may restart only the
// same immutable executable with the same descriptor and configuration.
// Provider configuration is sent over authenticated IPC and never placed in
// the child's environment or a public response.
type Runtime struct {
	mu     sync.Mutex
	opGate chan struct{}
	// readGate bounds response bodies retained after their serialized open
	// operation has returned. Provider control operations still use opGate.
	readGate chan struct{}

	client     *storageproto.Client
	directory  string
	objects    *objectStore
	descriptor storageproto.Descriptor

	binary              string
	expectedFingerprint string
	config              storageproto.Config
	lifetime            context.Context
	cancelLifetime      context.CancelFunc
	closed              bool
	closeOnce           sync.Once
	closeErr            error
	restartStreak       uint
	restartNotBefore    time.Time
}

// Start launches the exact immutable binary selected by the Host catalog,
// verifies its descriptor fingerprint, applies its private configuration,
// and requires its physical backend probe to pass before returning it for use.
func Start(ctx context.Context, binary string, expected storageproto.Descriptor, config storageproto.Config) (*Runtime, error) {
	if ctx == nil || !filepath.IsAbs(binary) || filepath.Clean(binary) != binary {
		return nil, errors.New("storage provider launch input is invalid")
	}
	if err := storageproto.ValidateDescriptor(expected); err != nil {
		return nil, errors.New("pinned storage provider descriptor is invalid")
	}
	if err := storageproto.ValidateConfigForSchema(expected.ConfigurationSchema, config); err != nil {
		return nil, errors.New("pinned storage provider configuration is invalid")
	}
	wantFingerprint, err := storageproto.Fingerprint(expected)
	if err != nil {
		return nil, errors.New("pinned storage provider descriptor is invalid")
	}
	child, actual, err := startChild(ctx, binary, expected, wantFingerprint, config)
	if err != nil {
		return nil, err
	}
	// The startup context can be a short-lived generation preparation context.
	// Runtime.Close is the owner-controlled lifetime boundary; detaching its
	// cancellation here prevents a successfully prepared child from becoming
	// unusable when that request-scoped context returns.
	lifetime, cancel := context.WithCancel(context.Background())
	runtime := &Runtime{
		client: child.client, directory: child.directory, descriptor: actual,
		binary: binary, expectedFingerprint: wantFingerprint, config: cloneConfig(config),
		lifetime: lifetime, cancelLifetime: cancel, opGate: make(chan struct{}, 1),
		readGate: make(chan struct{}, maxConcurrentReadStreams),
	}
	runtime.objects = &objectStore{runtime: runtime}
	return runtime, nil
}

type providerChild struct {
	client    *storageproto.Client
	directory string
}

// startChild always allocates fresh private IPC state. In particular, a
// crashed process may leave a stale Unix socket; the replacement never trusts
// or reuses that path.
func startChild(ctx context.Context, binary string, expected storageproto.Descriptor, expectedFingerprint string, config storageproto.Config) (_ providerChild, _ storageproto.Descriptor, resultErr error) {
	if ctx == nil {
		return providerChild{}, storageproto.Descriptor{}, errors.New("storage provider launch context is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return providerChild{}, storageproto.Descriptor{}, err
	}
	directory, err := os.MkdirTemp("/tmp", "irsp-")
	if err != nil {
		return providerChild{}, storageproto.Descriptor{}, errors.New("private storage provider IPC directory is unavailable")
	}
	if err := os.Chmod(directory, 0700); err != nil {
		_ = removeOwnedDirectory(directory)
		return providerChild{}, storageproto.Descriptor{}, errors.New("private storage provider IPC directory is unavailable")
	}
	keepDirectory := false
	defer func() {
		if !keepDirectory {
			if err := removeOwnedDirectory(directory); resultErr == nil && err != nil {
				resultErr = errors.New("private storage provider IPC cleanup failed")
			}
		}
	}()
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return providerChild{}, storageproto.Descriptor{}, errors.New("storage provider IPC identity could not be created")
	}
	suffix := hex.EncodeToString(nonce[:])
	socketPath := filepath.Join(directory, "p.sock")
	tokenPath := filepath.Join(directory, "t-"+suffix)
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return providerChild{}, storageproto.Descriptor{}, errors.New("storage provider IPC credential could not be created")
	}
	tokenText := []byte(hex.EncodeToString(token[:]))
	file, err := os.OpenFile(tokenPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		zeroBytes(tokenText)
		zeroBytes(token[:])
		return providerChild{}, storageproto.Descriptor{}, errors.New("storage provider IPC credential could not be secured")
	}
	if _, err := file.Write(tokenText); err != nil {
		_ = file.Close()
		zeroBytes(tokenText)
		zeroBytes(token[:])
		return providerChild{}, storageproto.Descriptor{}, errors.New("storage provider IPC credential could not be secured")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		zeroBytes(tokenText)
		zeroBytes(token[:])
		return providerChild{}, storageproto.Descriptor{}, errors.New("storage provider IPC credential could not be secured")
	}
	if err := file.Close(); err != nil {
		zeroBytes(tokenText)
		zeroBytes(token[:])
		return providerChild{}, storageproto.Descriptor{}, errors.New("storage provider IPC credential could not be secured")
	}
	zeroBytes(tokenText)
	zeroBytes(token[:])
	client, err := storageproto.Start(ctx, storageproto.StartOptions{
		Binary: binary, SocketPath: socketPath, TokenFile: tokenPath, StartupTimeout: 8 * time.Second,
	})
	if err != nil {
		return providerChild{}, storageproto.Descriptor{}, errors.New("pinned storage provider process could not start")
	}
	keepClient := false
	defer func() {
		if !keepClient {
			_ = client.Close()
		}
	}()
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	actual, err := client.Describe(probeCtx)
	cancel()
	if err != nil {
		return providerChild{}, storageproto.Descriptor{}, errors.New("pinned storage provider descriptor is unavailable")
	}
	gotFingerprint, err := storageproto.Fingerprint(actual)
	if err != nil || expectedFingerprint == "" || gotFingerprint != expectedFingerprint {
		return providerChild{}, storageproto.Descriptor{}, errors.New("pinned storage provider identity changed")
	}
	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	configured := cloneConfig(config)
	err = client.Configure(callCtx, configured)
	zeroConfig(&configured)
	cancel()
	if err != nil {
		return providerChild{}, storageproto.Descriptor{}, errors.New("pinned storage provider configuration failed")
	}
	callCtx, cancel = context.WithTimeout(ctx, 30*time.Second)
	err = client.Probe(callCtx)
	cancel()
	if err != nil {
		return providerChild{}, storageproto.Descriptor{}, errors.New("pinned storage provider probe failed")
	}
	if err := ctx.Err(); err != nil {
		return providerChild{}, storageproto.Descriptor{}, err
	}
	keepClient, keepDirectory = true, true
	return providerChild{client: client, directory: directory}, actual, nil
}

// StartSet opens the Host-owned immutable storage-provider set selected by an
// application generation. It never consults mutable Registry artifacts or
// external adapter directories.
func StartSet(ctx context.Context, catalogRoot, setID string) (*Runtime, error) {
	if ctx == nil || !filepath.IsAbs(catalogRoot) || filepath.Clean(catalogRoot) != catalogRoot || setID == "" {
		return nil, errors.New("pinned storage provider set is invalid")
	}
	catalog, err := storagecatalog.OpenReadOnly(catalogRoot)
	if err != nil {
		return nil, errors.New("pinned storage provider catalog is unavailable")
	}
	set, err := catalog.LoadSet(setID)
	if err != nil {
		return nil, errors.New("pinned storage provider set is unavailable")
	}
	binary, err := catalog.ArtifactPath(set.Artifact.Digest)
	if err != nil {
		return nil, errors.New("pinned storage provider artifact is unavailable")
	}
	artifact, descriptor, err := catalog.DescribeArtifact(set.Artifact.Digest)
	if err != nil || artifact.ID != set.Artifact.ID || artifact.Version != set.Artifact.Version || artifact.ProtocolVersion != set.Artifact.ProtocolVersion || artifact.Digest != set.Artifact.Digest {
		return nil, errors.New("pinned storage provider identity is unavailable")
	}
	return Start(ctx, binary, descriptor, storageproto.Config{Values: set.Config.Values, Secrets: set.Config.Secrets})
}

// OpenStore opens the exact provider set pinned by a Runtime Host generation.
// Empty set identity is rejected: the production application path has no
// direct-local or fresh-archive fallback.
func OpenStore(ctx context.Context, dataDir, catalogRoot, setID string) (*storage.Store, *Runtime, error) {
	if strings.TrimSpace(dataDir) == "" || setID == "" || catalogRoot == "" {
		return nil, nil, errors.New("archive storage root is unavailable")
	}
	if !filepath.IsAbs(dataDir) || filepath.Clean(dataDir) != dataDir || !filepath.IsAbs(catalogRoot) || filepath.Clean(catalogRoot) != catalogRoot {
		return nil, nil, errors.New("storage provider catalog root is invalid")
	}
	expectedRoot := filepath.Join(dataDir, "runtime", "storage-providers")
	if catalogRoot != expectedRoot {
		return nil, nil, errors.New("storage provider catalog root does not match the Runtime Host")
	}
	provider, err := StartSet(ctx, catalogRoot, setID)
	if err != nil {
		return nil, nil, err
	}
	store, err := storage.NewWithObjectStore(dataDir, provider.ObjectStore())
	if err != nil {
		_ = provider.Close()
		return nil, nil, err
	}
	return store, provider, nil
}

func (r *Runtime) ObjectStore() storage.PhysicalObjectStore {
	if r == nil {
		return nil
	}
	return r.objects
}

func (r *Runtime) Descriptor() storageproto.Descriptor {
	if r == nil {
		return storageproto.Descriptor{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneDescriptor(r.descriptor)
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		if r.cancelLifetime != nil {
			r.cancelLifetime()
		}
		r.mu.Lock()
		r.closed = true
		client, directory := r.client, r.directory
		r.client, r.directory = nil, ""
		zeroConfig(&r.config)
		r.mu.Unlock()
		if client != nil {
			r.closeErr = errors.Join(r.closeErr, client.Close())
		}
		if directory != "" && (client == nil || client.Exited()) {
			r.closeErr = errors.Join(r.closeErr, removeOwnedDirectory(directory))
		} else if directory != "" {
			r.closeErr = errors.Join(r.closeErr, errors.New("storage provider process did not exit; private IPC directory retained"))
		}
	})
	if r.closeErr != nil {
		return errors.New("storage provider process cleanup failed")
	}
	return nil
}

type objectStore struct{ runtime *Runtime }

func (s *objectStore) StorageProviderIdentity() (id, name string) {
	if s == nil || s.runtime == nil {
		return "", ""
	}
	descriptor := s.runtime.Descriptor()
	return descriptor.ID, descriptor.Name
}

func (s *objectStore) Put(ctx context.Context, key string, body io.Reader, size int64) (storage.PhysicalObjectInfo, error) {
	var info storageproto.ObjectInfo
	err := s.runtime.call(ctx, func(callCtx context.Context, client *storageproto.Client) error {
		var err error
		info, err = client.Put(callCtx, key, body, size)
		return err
	})
	if err != nil {
		return storage.PhysicalObjectInfo{}, err
	}
	return storage.PhysicalObjectInfo{Key: key, Size: info.Size, SHA256: info.SHA256, ModifiedAt: time.Now().UTC()}, nil
}

func (s *objectStore) Open(ctx context.Context, key string) (io.ReadCloser, storage.PhysicalObjectInfo, error) {
	return s.open(ctx, key, func(callCtx context.Context, client *storageproto.Client) (io.ReadCloser, storageproto.ObjectInfo, error) {
		return client.Open(callCtx, key)
	})
}

func (s *objectStore) OpenRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.PhysicalObjectInfo, error) {
	return s.open(ctx, key, func(callCtx context.Context, client *storageproto.Client) (io.ReadCloser, storageproto.ObjectInfo, error) {
		return client.OpenRange(callCtx, key, offset, length)
	})
}

func (s *objectStore) open(ctx context.Context, key string, open func(context.Context, *storageproto.Client) (io.ReadCloser, storageproto.ObjectInfo, error)) (io.ReadCloser, storage.PhysicalObjectInfo, error) {
	if s == nil || s.runtime == nil {
		return nil, storage.PhysicalObjectInfo{}, storageproto.ErrClosed
	}
	r := s.runtime
	callCtx, cancel := r.operationContext(ctx)
	if err := r.acquireReadStream(callCtx); err != nil {
		cancel()
		return nil, storage.PhysicalObjectInfo{}, err
	}
	if err := r.acquireOperation(callCtx); err != nil {
		r.releaseReadStream()
		cancel()
		return nil, storage.PhysicalObjectInfo{}, err
	}
	client, err := r.ensureClient(callCtx)
	if err != nil {
		r.releaseOperation()
		r.releaseReadStream()
		cancel()
		return nil, storage.PhysicalObjectInfo{}, err
	}
	reader, info, err := open(callCtx, client)
	if err != nil {
		r.recoverAfterOperationError(callCtx, client)
		r.releaseOperation()
		r.releaseReadStream()
		cancel()
		return nil, storage.PhysicalObjectInfo{}, normalizeProviderObjectError(err)
	}
	r.markOperationSucceeded()
	stream := &operationReadCloser{ReadCloser: reader, release: func() {
		cancel()
		r.releaseReadStream()
	}}
	stream.watchContext(callCtx)
	r.releaseOperation()
	return stream, storage.PhysicalObjectInfo{Key: key, Size: info.Size, SHA256: info.SHA256}, nil
}

func (s *objectStore) Stat(ctx context.Context, key string) (storage.PhysicalObjectInfo, error) {
	var info storageproto.ObjectInfo
	err := s.runtime.call(ctx, func(callCtx context.Context, client *storageproto.Client) error {
		var err error
		info, err = client.Stat(callCtx, key)
		return err
	})
	if err != nil {
		return storage.PhysicalObjectInfo{}, normalizeProviderObjectError(err)
	}
	return storage.PhysicalObjectInfo{Key: key, Size: info.Size, SHA256: info.SHA256}, nil
}

func (s *objectStore) List(ctx context.Context, prefix, cursor string, limit int) (storage.PhysicalObjectPage, error) {
	var page storageproto.ListPage
	err := s.runtime.call(ctx, func(callCtx context.Context, client *storageproto.Client) error {
		var err error
		page, err = client.List(callCtx, prefix, cursor, limit)
		return err
	})
	if err != nil {
		return storage.PhysicalObjectPage{}, err
	}
	items := make([]storage.PhysicalObjectInfo, 0, len(page.Items))
	last := cursor
	for _, item := range page.Items {
		if item.Key <= last || prefix != "" && !strings.HasPrefix(item.Key, prefix) || item.Size < 0 || item.Size > storage.MaxObjectBytes {
			return storage.PhysicalObjectPage{}, errors.New("storage provider returned an invalid object page")
		}
		last = item.Key
		items = append(items, storage.PhysicalObjectInfo{Key: item.Key, Size: item.Size, SHA256: item.SHA256})
	}
	if page.NextCursor != "" && (len(items) == 0 || page.NextCursor != last) {
		return storage.PhysicalObjectPage{}, errors.New("storage provider returned an invalid object cursor")
	}
	if !sort.SliceIsSorted(items, func(i, j int) bool { return items[i].Key < items[j].Key }) {
		return storage.PhysicalObjectPage{}, errors.New("storage provider returned an unordered object page")
	}
	return storage.PhysicalObjectPage{Items: items, NextCursor: page.NextCursor}, nil
}

func (s *objectStore) Delete(ctx context.Context, key string) error {
	err := s.runtime.call(ctx, func(callCtx context.Context, client *storageproto.Client) error { return client.Delete(callCtx, key) })
	if errors.Is(err, storageproto.ErrNotFound) {
		return nil // Delete has idempotent semantics at the Core boundary.
	}
	return err
}

func (r *Runtime) call(ctx context.Context, operation func(context.Context, *storageproto.Client) error) error {
	if r == nil || operation == nil {
		return storageproto.ErrClosed
	}
	callCtx, cancel := r.operationContext(ctx)
	defer cancel()
	if err := r.acquireOperation(callCtx); err != nil {
		return err
	}
	defer r.releaseOperation()
	client, err := r.ensureClient(callCtx)
	if err != nil {
		return err
	}
	err = operation(callCtx, client)
	if err == nil {
		r.markOperationSucceeded()
		return nil
	}
	r.recoverAfterOperationError(callCtx, client)
	return err
}

func (r *Runtime) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	stop := context.AfterFunc(r.lifetime, cancel)
	return callCtx, func() {
		stop()
		cancel()
	}
}

func (r *Runtime) acquireOperation(ctx context.Context) error {
	if r == nil || r.opGate == nil {
		return storageproto.ErrClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.lifetime.Done():
		return storageproto.ErrClosed
	case r.opGate <- struct{}{}:
		return nil
	}
}

func (r *Runtime) acquireReadStream(ctx context.Context) error {
	if r == nil || r.readGate == nil {
		return storageproto.ErrClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.lifetime.Done():
		return storageproto.ErrClosed
	case r.readGate <- struct{}{}:
		return nil
	}
}

func (r *Runtime) releaseOperation() {
	<-r.opGate
}

func (r *Runtime) releaseReadStream() {
	<-r.readGate
}

func (r *Runtime) ensureClient(ctx context.Context) (*storageproto.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.lifetime.Err() != nil {
		return nil, storageproto.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.client != nil && !r.client.Exited() {
		return r.client, nil
	}
	if r.client != nil {
		client, directory := r.client, r.directory
		r.client, r.directory = nil, ""
		_ = client.Close()
		if client.Exited() {
			_ = removeOwnedDirectory(directory)
		}
		r.noteRestartFailureLocked()
	}
	if wait := time.Until(r.restartNotBefore); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.lifetime.Done():
			return nil, storageproto.ErrClosed
		case <-timer.C:
		}
	}
	child, actual, err := startChild(ctx, r.binary, r.descriptor, r.expectedFingerprint, r.config)
	if err != nil {
		r.noteRestartFailureLocked()
		return nil, err
	}
	r.client, r.directory, r.descriptor = child.client, child.directory, actual
	return r.client, nil
}

func (r *Runtime) noteRestartFailureLocked() {
	if r.restartStreak < 16 {
		r.restartStreak++
	}
	delay := initialRestartBackoff
	for i := uint(1); i < r.restartStreak && delay < maximumRestartBackoff; i++ {
		delay *= 2
	}
	if delay > maximumRestartBackoff {
		delay = maximumRestartBackoff
	}
	r.restartNotBefore = time.Now().Add(delay)
}

func (r *Runtime) markOperationSucceeded() {
	r.mu.Lock()
	r.restartStreak = 0
	r.restartNotBefore = time.Time{}
	r.mu.Unlock()
}

func (r *Runtime) recoverAfterOperationError(ctx context.Context, client *storageproto.Client) {
	if client == nil || ctx.Err() != nil || r.lifetime.Err() != nil {
		return
	}
	if !client.Exited() {
		done := client.Done()
		if done == nil {
			return
		}
		timer := time.NewTimer(exitObservationWindow)
		defer timer.Stop()
		select {
		case <-done:
		case <-ctx.Done():
			return
		case <-r.lifetime.Done():
			return
		case <-timer.C:
			return
		}
	}
	_, _ = r.ensureClient(ctx)
}

func removeOwnedDirectory(path string) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Dir(path) != "/tmp" || !strings.HasPrefix(filepath.Base(path), "irsp-") {
		return errors.New("private storage provider IPC directory is invalid")
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("private storage provider IPC directory is unsafe")
	}
	return os.RemoveAll(path)
}

func cloneConfig(config storageproto.Config) storageproto.Config {
	copy := storageproto.Config{
		Values:  make(map[string]json.RawMessage, len(config.Values)),
		Secrets: make(map[string]string, len(config.Secrets)),
	}
	for key, value := range config.Values {
		copy.Values[key] = append(json.RawMessage(nil), value...)
	}
	for key, value := range config.Secrets {
		copy.Secrets[key] = value
	}
	return copy
}

func zeroConfig(config *storageproto.Config) {
	if config == nil {
		return
	}
	for key, value := range config.Values {
		zeroBytes(value)
		delete(config.Values, key)
	}
	for key := range config.Secrets {
		config.Secrets[key] = ""
		delete(config.Secrets, key)
	}
}

func zeroBytes(value []byte) {
	for i := range value {
		value[i] = 0
	}
}

func cloneDescriptor(descriptor storageproto.Descriptor) storageproto.Descriptor {
	copy := descriptor
	copy.Capabilities = append([]string(nil), descriptor.Capabilities...)
	copy.ConfigurationSchema.Fields = append([]storageproto.Field(nil), descriptor.ConfigurationSchema.Fields...)
	return copy
}

type operationReadCloser struct {
	io.ReadCloser
	release  func()
	once     sync.Once
	stopMu   sync.Mutex
	stop     func() bool
	closeErr error
}

func (r *operationReadCloser) watchContext(ctx context.Context) {
	stop := context.AfterFunc(ctx, func() { _ = r.Close() })
	r.stopMu.Lock()
	r.stop = stop
	r.stopMu.Unlock()
}

func (r *operationReadCloser) finish() {
	r.once.Do(func() {
		r.stopMu.Lock()
		stop := r.stop
		r.stopMu.Unlock()
		if stop != nil {
			stop()
		}
		r.closeErr = r.ReadCloser.Close()
		r.release()
	})
}

func (r *operationReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err != nil {
		r.finish()
	}
	return n, err
}

func (r *operationReadCloser) Close() error {
	r.finish()
	return r.closeErr
}

func normalizeProviderObjectError(err error) error {
	if errors.Is(err, storageproto.ErrNotFound) {
		return storage.ErrObjectNotFound
	}
	return err
}

var _ storage.PhysicalObjectStore = (*objectStore)(nil)
