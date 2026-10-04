package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/integrated-recorder/core/internal/storage"
)

func TestConfigureRuntimeResourcesRequiresAllOrNone(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{"RUNTIME_RESOURCE_SOCKET_PATH": "/run/runtime/resources.sock"}
	if err := configureRuntimeResources(store, func(key string) string { return values[key] }); err == nil {
		t.Fatal("partial runtime resource configuration was accepted")
	}
	if err := configureRuntimeResources(store, func(string) string { return "" }); err != nil {
		t.Fatalf("direct compatibility invocation rejected: %v", err)
	}
}

func TestConfigureRuntimeResourcesBindsPrivateHostTokenAndOwner(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(t.TempDir(), "resource-token")
	if err := os.WriteFile(tokenPath, make([]byte, 32), 0600); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{
		"RUNTIME_RESOURCE_SOCKET_PATH": "/run/runtime/resources.sock",
		"RUNTIME_RESOURCE_TOKEN_FILE":  tokenPath,
		"RUNTIME_RESOURCE_OWNER":       "engine-generation-a",
	}
	getenv := func(key string) string { return values[key] }
	if err := configureRuntimeResources(store, getenv); err != nil {
		t.Fatal(err)
	}
	if err := configureRuntimeResources(store, getenv); err == nil {
		t.Fatal("duplicate coordinator configuration was accepted")
	}
}

func TestConfigureRuntimeResourcesRejectsUnprivateToken(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(t.TempDir(), "resource-token")
	if err := os.WriteFile(tokenPath, make([]byte, 32), 0644); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{
		"RUNTIME_RESOURCE_SOCKET_PATH": "/run/runtime/resources.sock",
		"RUNTIME_RESOURCE_TOKEN_FILE":  tokenPath,
		"RUNTIME_RESOURCE_OWNER":       "engine-generation-a",
	}
	if err := configureRuntimeResources(store, func(key string) string { return values[key] }); err == nil {
		t.Fatal("non-private runtime resource token was accepted")
	}
}
