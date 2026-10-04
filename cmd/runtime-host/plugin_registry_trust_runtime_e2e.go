//go:build runtime_e2e

package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/runtimehost/bootstrap"
)

const runtimeE2ERegistryCAEnv = "IR_RUNTIME_E2E_PLUGIN_REGISTRY_CA_FILE"

// configureRuntimeE2EPluginRegistryTrust permits a process E2E to trust only
// its private local HTTPS fixture. Registry and artifact validation remain in
// the production Plugin Registry manager; no non-loopback origin can use this
// test-only CA injection path.
func configureRuntimeE2EPluginRegistryTrust(config *bootstrap.Config) error {
	if config == nil {
		return errors.New("missing host config")
	}
	caPath := strings.TrimSpace(os.Getenv(runtimeE2ERegistryCAEnv))
	if caPath == "" {
		return nil
	}
	u, err := url.Parse(config.PluginRegistryURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("test registry URL is invalid")
	}
	host := u.Hostname()
	if !strings.EqualFold(host, "localhost") {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return errors.New("test registry origin must be loopback")
		}
	}
	info, err := os.Lstat(caPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > 1<<20 {
		return errors.New("test CA file is unavailable")
	}
	data, err := os.ReadFile(caPath)
	if err != nil {
		return errors.New("test CA file is unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return errors.New("test CA file is invalid")
	}
	config.PluginRegistryHTTPClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			Proxy:           nil,
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots},
		},
	}
	return nil
}
