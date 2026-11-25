package main

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/discovery"
)

func TestLoadConfigDefaultsToLoopbackAndRequiresTLSAndDurableState(t *testing.T) {
	values := map[string]string{
		"IR_DISCOVERY_DATA_DIR":    filepath.Join(t.TempDir(), "state"),
		"IR_DISCOVERY_TLS_CERT":    filepath.Join(t.TempDir(), "cert.pem"),
		"IR_DISCOVERY_TLS_KEY":     filepath.Join(t.TempDir(), "key.pem"),
		"IR_DISCOVERY_LISTEN_ADDR": "",
	}
	cfg, err := loadConfig(func(key string) string { return values[key] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listenAddress != defaultListenAddress {
		t.Fatalf("default listen address=%q, want %q", cfg.listenAddress, defaultListenAddress)
	}
	delete(values, "IR_DISCOVERY_TLS_KEY")
	if _, err := loadConfig(func(key string) string { return values[key] }); err == nil {
		t.Fatal("accepted missing TLS private key")
	}
}

func TestLoadConfigAllowsExplicitAddressButRejectsUnsafePathsAndAddresses(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state with spaces")
	values := map[string]string{
		"IR_DISCOVERY_LISTEN_ADDR": "100.78.57.60:9443",
		"IR_DISCOVERY_DATA_DIR":    root,
		"IR_DISCOVERY_TLS_CERT":    filepath.Join(t.TempDir(), "cert.pem"),
		"IR_DISCOVERY_TLS_KEY":     filepath.Join(t.TempDir(), "key.pem"),
	}
	get := func(key string) string { return values[key] }
	cfg, err := loadConfig(get)
	if err != nil || cfg.listenAddress != values["IR_DISCOVERY_LISTEN_ADDR"] || cfg.dataRoot != root {
		t.Fatalf("explicit config=%+v err=%v", cfg, err)
	}
	uncleanParent := root + string(filepath.Separator) + ".." + string(filepath.Separator) + "state"
	for _, invalid := range []string{"relative/state", string(filepath.Separator), uncleanParent} {
		values["IR_DISCOVERY_DATA_DIR"] = invalid
		if _, err := loadConfig(get); err == nil {
			t.Errorf("accepted unsafe state path %q", invalid)
		}
	}
	values["IR_DISCOVERY_DATA_DIR"] = root
	for _, invalid := range []string{"not-an-address", "0.0.0.0:bad", "100.78.57.60:9443\nHost:evil"} {
		values["IR_DISCOVERY_LISTEN_ADDR"] = invalid
		if _, err := loadConfig(get); err == nil {
			t.Errorf("accepted invalid listen address %q", invalid)
		}
	}
}

func TestServerProvidesHealthAndRoutesDirectoryWithoutCORS(t *testing.T) {
	directory, err := discovery.OpenDirectory(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	server := newServer(defaultListenAddress, directory)

	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	server.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != "ok" || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("health status=%d headers=%v body=%q", response.Code, response.Header(), response.Body.String())
	}

	request = httptest.NewRequest(http.MethodOptions, "/v1/advertisements", nil)
	response = httptest.NewRecorder()
	server.Handler.ServeHTTP(response, request)
	if response.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("directory unexpectedly enables CORS: %v", response.Header())
	}
}
