//go:build runtime_e2e

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
)

// runtimeE2EFixtureOrigin is empty unless the acceptance-test build explicitly
// injects one exact loopback origin with -ldflags. It is not operator-configurable.
var runtimeE2EFixtureOrigin string

func newAcquisitionClient(timeout time.Duration) (*http.Client, acquire.SourceValidator) {
	if timeout <= 0 {
		timeout = 25 * time.Second
	}
	origin, err := parseRuntimeE2EOrigin(runtimeE2EFixtureOrigin)
	if err != nil {
		transport := &http.Transport{Proxy: nil, DialContext: func(context.Context, string, string) (net.Conn, error) {
			return nil, errors.New("runtime E2E fixture origin is unavailable")
		}}
		return &http.Client{Timeout: timeout, Transport: transport}, func(context.Context, string) error { return err }
	}
	port := origin.Port()
	dialer := &net.Dialer{Timeout: 8 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                 nil,
		ResponseHeaderTimeout: 10 * time.Second,
		IdleConnTimeout:       30 * time.Second,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, rawPort, splitErr := net.SplitHostPort(address)
			if splitErr != nil || host != "127.0.0.1" || rawPort != port {
				return nil, errors.New("runtime E2E client refused a non-fixture destination")
			}
			return dialer.DialContext(ctx, network, address)
		},
	}
	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(request *http.Request, _ []*http.Request) error {
			if err := validateRuntimeE2EURL(request.URL.String(), origin); err != nil {
				return errors.New("runtime E2E redirect left the fixture origin")
			}
			return nil
		},
	}
	return client, func(_ context.Context, raw string) error { return validateRuntimeE2EURL(raw, origin) }
}

func parseRuntimeE2EOrigin(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("runtime E2E fixture origin must be one exact loopback HTTP origin")
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 || raw != "http://127.0.0.1:"+u.Port() {
		return nil, errors.New("runtime E2E fixture origin port is invalid")
	}
	return u, nil
}

func validateRuntimeE2EURL(raw string, origin *url.URL) error {
	u, err := url.ParseRequestURI(raw)
	if err != nil || origin == nil || u.Scheme != "http" || u.Host != origin.Host || u.Hostname() != "127.0.0.1" || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("source URL is outside the runtime E2E fixture origin")
	}
	return nil
}
