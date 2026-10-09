package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/recorderengine"
	"github.com/integrated-recorder/core/internal/runtimeipc"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestDecodeEngineCatalogStrictAndBounded(t *testing.T) {
	cases := []struct {
		name string
		data string
	}{
		{"unknown field", `{"version":1,"active_generation_id":"engine-a","engines":[],"ignored":true}`},
		{"unsupported version", `{"version":2,"active_generation_id":"engine-a","engines":[{"generation_id":"engine-a","socket_path":"/tmp/x","token_file":"/tmp/y","instance_id":"instance-a"}]}`},
		{"trailing json", `{"version":1,"active_generation_id":"engine-a","engines":[]} {}`},
		{"oversized", strings.Repeat("x", maxEngineCatalogSize+1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeEngineCatalog([]byte(tc.data)); err == nil {
				t.Fatal("invalid catalog unexpectedly accepted")
			}
		})
	}
}

func TestEngineCatalogValidatesPathsIdentitiesAndCardinality(t *testing.T) {
	private := shortPrivateTempDir(t)
	socket, tokenPath := startCatalogFixture(t, private, "engine-a", "instance-a")
	valid := EngineCatalog{Version: 1, ActiveGenerationID: "engine-a", Engines: []EngineCatalogEntry{{GenerationID: "engine-a", SocketPath: socket, TokenFile: tokenPath, InstanceID: "instance-a"}}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid catalog: %v", err)
	}

	invalid := []EngineCatalog{
		{Version: 1, ActiveGenerationID: "../engine-a", Engines: valid.Engines},
		{Version: 1, ActiveGenerationID: "engine-b", Engines: valid.Engines},
		{Version: 1, ActiveGenerationID: "engine-a", Engines: append(valid.Engines, valid.Engines[0])},
		{Version: 1, ActiveGenerationID: "engine-a", Engines: []EngineCatalogEntry{{GenerationID: "engine-a", SocketPath: filepath.Join(private, "missing.sock"), TokenFile: tokenPath, InstanceID: "instance-a"}}},
	}
	tooMany := EngineCatalog{Version: 1, ActiveGenerationID: "engine-a", Engines: make([]EngineCatalogEntry, maxEngineCatalogItems+1)}
	invalid = append(invalid, tooMany)
	for i, item := range invalid {
		if err := item.Validate(); err == nil {
			t.Fatalf("invalid catalog case %d unexpectedly accepted", i)
		}
	}
}

func TestLoadEngineCatalogRejectsTokenSymlinkAndInsecurePermissions(t *testing.T) {
	private := shortPrivateTempDir(t)
	socket, tokenPath := startCatalogFixture(t, private, "engine-a", "instance-a")
	alias := filepath.Join(private, "alias.token")
	if err := os.Symlink(tokenPath, alias); err != nil {
		t.Fatal(err)
	}
	catalog := EngineCatalog{Version: 1, ActiveGenerationID: "engine-a", Engines: []EngineCatalogEntry{{GenerationID: "engine-a", SocketPath: socket, TokenFile: alias, InstanceID: "instance-a"}}}
	if err := catalog.Validate(); err == nil {
		t.Fatal("token symlink unexpectedly accepted")
	}
	if err := os.Chmod(tokenPath, 0644); err != nil {
		t.Fatal(err)
	}
	catalog.Engines[0].TokenFile = tokenPath
	if err := catalog.Validate(); err == nil {
		t.Fatal("group/world-readable token unexpectedly accepted")
	}
}

func TestEngineCatalogWiresInstancePinnedClientsToManagerRouter(t *testing.T) {
	private := shortPrivateTempDir(t)
	socket, tokenPath := startCatalogFixture(t, private, "engine-a", "instance-a")
	catalogPath := filepath.Join(private, "catalog.json")
	data, err := json.Marshal(EngineCatalog{Version: 1, ActiveGenerationID: "engine-a", Engines: []EngineCatalogEntry{{GenerationID: "engine-a", SocketPath: socket, TokenFile: tokenPath, InstanceID: "instance-a"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(catalogPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	store, err := storage.New(filepath.Join(private, "data"))
	if err != nil {
		t.Fatal(err)
	}
	router, err := LoadManagerRouter(catalogPath, store)
	if err != nil {
		t.Fatalf("load manager router: %v", err)
	}
	rows, err := router.ListForManagement(context.Background(), 10)
	if err != nil {
		t.Fatalf("engine client call through router: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("empty engine list returned %d rows", len(rows))
	}
}

func shortPrivateTempDir(t *testing.T) string {
	t.Helper()
	for _, base := range []string{"/private/tmp", "/tmp", os.TempDir()} {
		info, err := os.Lstat(base)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		path, err := os.MkdirTemp(base, "ircp-")
		if err == nil {
			if err := os.Chmod(path, 0700); err != nil {
				_ = os.RemoveAll(path)
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(path) })
			return path
		}
	}
	t.Fatal("no short private temporary directory is available")
	return ""
}

type configFixtureEngine struct {
	generation string
	instance   string
}

func (h configFixtureEngine) GenerationID() string      { return h.generation }
func (h configFixtureEngine) RuntimeInstanceID() string { return h.instance }
func (h configFixtureEngine) Handle(ctx context.Context, operation string, _ json.RawMessage) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch operation {
	case "ready":
		return recorderengine.ReadyResult{Ready: true, GenerationID: h.generation, InstanceID: h.instance, ProtocolVersion: runtimeipc.ProtocolVersion}, nil
	case recorderengine.OperationInventory:
		return recorderengine.InventoryResult{GenerationID: h.generation, InstanceID: h.instance, Ready: true, Active: []recorderengine.ActiveRecording{}}, nil
	case recorderengine.OperationList:
		return []*domain.Recording{}, nil
	case recorderengine.OperationInvalidateManagementRootCache:
		return struct {
			Invalidated bool `json:"invalidated"`
		}{Invalidated: true}, nil
	default:
		return nil, errors.New("unexpected operation")
	}
}

func startCatalogFixture(t *testing.T, private, generation, instance string) (socketPath, tokenPath string) {
	t.Helper()
	var token [32]byte
	for i := range token {
		token[i] = byte(i + 1)
	}
	tokenPath = filepath.Join(private, generation+".token")
	if err := os.WriteFile(tokenPath, token[:], 0600); err != nil {
		t.Fatal(err)
	}
	socketPath = filepath.Join(private, generation+".sock")
	ipcServer, err := runtimeipc.NewServer(socketPath, generation, token[:], configFixtureEngine{generation: generation, instance: instance})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ipcServer.Serve(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, dialErr := net.DialTimeout("unix", socketPath, 10*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("fixture IPC listener did not start")
		}
		time.Sleep(time.Millisecond)
	}
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("fixture IPC shutdown: %v", err)
		}
	})
	return socketPath, tokenPath
}
