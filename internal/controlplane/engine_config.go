package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/runtimeipc"
	"github.com/integrated-recorder/core/internal/storage"
)

const (
	engineCatalogVersion  = 1
	maxEngineCatalogSize  = 256 << 10
	maxEngineCatalogItems = 32
)

var runtimeIdentity = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// EngineCatalog is the bounded, private attachment list shared by a Control
// generation and the Runtime Host. It contains no recording or media state.
type EngineCatalog struct {
	Version            int                  `json:"version"`
	ActiveGenerationID string               `json:"active_generation_id"`
	Engines            []EngineCatalogEntry `json:"engines"`
}

// EngineCatalogEntry pins a client to one exact engine process instance.
type EngineCatalogEntry struct {
	GenerationID string `json:"generation_id"`
	SocketPath   string `json:"socket_path"`
	TokenFile    string `json:"token_file"`
	InstanceID   string `json:"instance_id"`
}

// LoadEngineCatalog reads and validates the Runtime Host's atomic catalog.
// The catalog and its credentials must live below a private IPC directory;
// socket and token files are checked again immediately before use.
func LoadEngineCatalog(path string) (EngineCatalog, error) {
	var catalog EngineCatalog
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) {
		return catalog, errors.New("engine catalog path must be an absolute clean path")
	}
	if err := validatePrivateParent(filepath.Dir(path)); err != nil {
		return catalog, errors.New("engine catalog directory is not private")
	}
	info, err := os.Lstat(path)
	if err != nil || !privateReadFile(info) || info.Size() <= 0 || info.Size() > maxEngineCatalogSize {
		return catalog, errors.New("engine catalog file is unavailable or unsafe")
	}
	file, err := os.Open(path)
	if err != nil {
		return catalog, errors.New("engine catalog file is unavailable")
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil || !os.SameFile(info, openedInfo) {
		return catalog, errors.New("engine catalog file changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxEngineCatalogSize+1))
	if err != nil || len(data) == 0 || len(data) > maxEngineCatalogSize {
		return EngineCatalog{}, errors.New("engine catalog exceeds its size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&catalog); err != nil {
		return EngineCatalog{}, errors.New("engine catalog is malformed")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return EngineCatalog{}, errors.New("engine catalog has trailing data")
	}
	if err := catalog.Validate(); err != nil {
		return EngineCatalog{}, err
	}
	return catalog, nil
}

// Validate checks all identities and private paths without opening engine
// connections. LoadEngineCatalog additionally checks that the catalog itself
// is a bounded private regular file.
func (c EngineCatalog) Validate() error {
	if c.Version != engineCatalogVersion || !runtimeIdentity.MatchString(c.ActiveGenerationID) || len(c.Engines) == 0 || len(c.Engines) > maxEngineCatalogItems {
		return errors.New("engine catalog version, active generation, or entry count is invalid")
	}
	seen := make(map[string]struct{}, len(c.Engines))
	seenSockets := make(map[string]struct{}, len(c.Engines))
	activeFound := false
	for _, engine := range c.Engines {
		if !runtimeIdentity.MatchString(engine.GenerationID) || !runtimeIdentity.MatchString(engine.InstanceID) {
			return errors.New("engine catalog identity is invalid")
		}
		if _, ok := seen[engine.GenerationID]; ok {
			return errors.New("engine catalog has duplicate generations")
		}
		seen[engine.GenerationID] = struct{}{}
		activeFound = activeFound || engine.GenerationID == c.ActiveGenerationID
		if err := validatePrivateSocket(engine.SocketPath); err != nil {
			return errors.New("engine catalog socket path is unavailable or unsafe")
		}
		if _, duplicate := seenSockets[engine.SocketPath]; duplicate {
			return errors.New("engine catalog has duplicate socket paths")
		}
		seenSockets[engine.SocketPath] = struct{}{}
		if _, err := readPrivateToken(engine.TokenFile); err != nil {
			return errors.New("engine catalog token file is unavailable or unsafe")
		}
	}
	if !activeFound {
		return errors.New("active engine generation is not attached")
	}
	return nil
}

// NewManagerRouter reads one validated snapshot and builds generation-pinned
// Engine IPC clients. The returned router never owns or closes Engine
// processes; clients are stateless UDS callers.
func (c EngineCatalog) NewManagerRouter(store *storage.Store) (*recorderengine.ManagerRouter, error) {
	if store == nil {
		return nil, errors.New("archive read facade is required")
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	clients := make(map[string]*recorderengine.ManagerClient, len(c.Engines))
	for _, entry := range c.Engines {
		token, err := readPrivateToken(entry.TokenFile)
		if err != nil {
			return nil, errors.New("engine IPC credentials are unavailable")
		}
		client, err := runtimeipc.NewClientForInstance(entry.SocketPath, entry.GenerationID, entry.InstanceID, token, 15*time.Second)
		if err != nil {
			return nil, errors.New("engine IPC client configuration is invalid")
		}
		var ready recorderengine.ReadyResult
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = client.Call(ctx, recorderengine.OperationReady, nil, &ready)
		cancel()
		if err != nil || !ready.Ready || ready.GenerationID != entry.GenerationID || ready.InstanceID != entry.InstanceID || ready.ProtocolVersion != runtimeipc.ProtocolVersion {
			return nil, errors.New("engine is not ready or its identity/protocol does not match the catalog")
		}
		manager, err := recorderengine.NewManagerClient(client)
		if err != nil {
			return nil, errors.New("engine manager client could not be created")
		}
		clients[entry.GenerationID] = manager
	}
	return recorderengine.NewManagerRouter(c.ActiveGenerationID, clients, store)
}

// LoadPrivateIPCSecret reads a 32-byte credential from a private non-symlink
// file. It is exported for the Control Plane's own Host-facing IPC listener.
func LoadPrivateIPCSecret(path string) ([]byte, error) { return readPrivateToken(path) }

// LoadManagerRouter is the file-backed convenience used by the Control Plane
// executable. Catalog updates are picked up by new Control generations.
func LoadManagerRouter(path string, store *storage.Store) (*recorderengine.ManagerRouter, error) {
	catalog, err := LoadEngineCatalog(path)
	if err != nil {
		return nil, err
	}
	return catalog.NewManagerRouter(store)
}

func readPrivateToken(path string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) {
		return nil, errors.New("token path is invalid")
	}
	if err := validatePrivateParent(filepath.Dir(path)); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil || !privateReadFile(info) || info.Size() != 32 {
		return nil, errors.New("token file is unsafe")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("token file is invalid")
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil || !os.SameFile(info, openedInfo) {
		return nil, errors.New("token file changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(file, 33))
	if err != nil || len(data) != 32 {
		return nil, errors.New("token file is invalid")
	}
	return data, nil
}

func validatePrivateSocket(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) || len(path) > 100 {
		return errors.New("socket path is invalid")
	}
	if err := validatePrivateParent(filepath.Dir(path)); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("socket path is not an existing socket")
	}
	return nil
}

func validatePrivateParent(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("private directory path is invalid")
	}
	if err := rejectSymlinkComponents(path); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 {
		return errors.New("private directory must be a non-symlink 0700 directory")
	}
	return nil
}

func privateReadFile(info os.FileInfo) bool {
	return info != nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 && info.Mode().Perm()&0077 == 0 && info.Mode().Perm()&0111 == 0 && info.Mode().Perm()&0400 != 0
}

func rejectSymlinkComponents(path string) error {
	volume := filepath.VolumeName(path)
	current := volume + string(filepath.Separator)
	rel := strings.TrimPrefix(path, current)
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("path contains a symlink or unavailable component")
		}
	}
	return nil
}

// DecodeEngineCatalog is useful to test callers that already hold an atomic
// catalog blob. It retains the same strict shape and entry validation as the
// file-backed loader.
func DecodeEngineCatalog(data []byte) (EngineCatalog, error) {
	if len(data) == 0 || len(data) > maxEngineCatalogSize {
		return EngineCatalog{}, errors.New("engine catalog size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var catalog EngineCatalog
	if err := decoder.Decode(&catalog); err != nil {
		return EngineCatalog{}, errors.New("engine catalog is malformed")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return EngineCatalog{}, errors.New("engine catalog has trailing data")
	}
	if err := catalog.Validate(); err != nil {
		return EngineCatalog{}, err
	}
	return catalog, nil
}
