package tlsserver

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSelfSignedHTTPSListenerRedirectPersistenceAndDisable(t *testing.T) {
	root := t.TempDir()
	m, err := Open(root, "127.0.0.1:0", 19443)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close(testContext(t))
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, r.URL.Path) })
	wrapped := m.WrapHTTP(mux)
	if err := m.SetHandler(wrapped); err != nil {
		t.Fatal(err)
	}
	if err := m.EnableSelfSigned([]string{"localhost", "127.0.0.1"}, true); err != nil {
		t.Fatal(err)
	}
	status := m.Status()
	if !status.Enabled || !status.Listening || !status.RedirectHTTP || status.Port != 19443 || !status.SelfSigned {
		t.Fatalf("unexpected status: %+v", status)
	}
	if !status.Configured || !status.RedirectActive {
		t.Fatalf("saved configuration and active redirect were not distinguished: %+v", status)
	}
	hostsBefore := strings.Join(status.Hosts, ",")
	cert, err := os.ReadFile(filepath.Join(root, "security", "https.json"))
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "security", "https.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("private TLS state mode = %o", info.Mode().Perm())
	}
	if !strings.Contains(string(cert), "PRIVATE KEY") {
		t.Fatal("test fixture does not contain persisted certificate pair")
	}
	listener := m.listener
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, Timeout: 3 * time.Second}
	resp, err := client.Get("https://" + listener.Addr().String() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "ok" {
		t.Fatalf("HTTPS response = %d %q", resp.StatusCode, body)
	}
	for _, path := range []string{"/", "/setup", "/login", "/system", "/static/ui/assets/app.js", "/api/setup/status", "/api/runtime/https", "/unknown"} {
		redirect := httptest.NewRecorder()
		wrapped.ServeHTTP(redirect, httptest.NewRequest(http.MethodGet, "http://localhost"+path+"?q=1", nil))
		if redirect.Code != http.StatusTemporaryRedirect || redirect.Header().Get("Location") != "https://localhost:19443"+path+"?q=1" || redirect.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("redirect %s = %d location=%q cache=%q", path, redirect.Code, redirect.Header().Get("Location"), redirect.Header().Get("Cache-Control"))
		}
	}
	health := httptest.NewRecorder()
	wrapped.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "http://localhost/healthz", nil))
	if health.Code != http.StatusOK || health.Header().Get("Location") != "" {
		t.Fatalf("health check should remain on HTTP: %d %q", health.Code, health.Header().Get("Location"))
	}
	if err := m.Disable(); err != nil {
		t.Fatal(err)
	}
	status = m.Status()
	if status.Enabled || status.Listening || !status.Configured || !status.RedirectHTTP || status.RedirectActive || !status.SelfSigned || strings.Join(status.Hosts, ",") != hostsBefore {
		t.Fatalf("disable did not preserve saved configuration while stopping listener/redirect: %+v", status)
	}
	for _, path := range []string{"/", "/system", "/static/ui/assets/app.js", "/api/setup/status"} {
		noRedirect := httptest.NewRecorder()
		wrapped.ServeHTTP(noRedirect, httptest.NewRequest(http.MethodGet, "http://localhost"+path, nil))
		if noRedirect.Code != http.StatusOK || noRedirect.Header().Get("Location") != "" {
			t.Fatalf("HTTP route %s remained redirected while HTTPS disabled: %d %q", path, noRedirect.Code, noRedirect.Header().Get("Location"))
		}
	}
	leafBefore, err := x509.ParseCertificate(m.cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	fingerprintBefore := sha256.Sum256(leafBefore.Raw)
	waitListenerClosed(t, listener.Addr().String())
	reopened, err := Open(root, "127.0.0.1:0", 19443)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(testContext(t))
	if err := reopened.SetHandler(reopened.WrapHTTP(mux)); err != nil {
		t.Fatal(err)
	}
	status = reopened.Status()
	if status.Enabled || status.Listening || !status.Configured || !status.RedirectHTTP || status.RedirectActive {
		t.Fatalf("restart auto-enabled HTTPS or lost saved settings: %+v", status)
	}
	leafAfterRestart, err := x509.ParseCertificate(reopened.cert.Certificate[0])
	if err != nil || sha256.Sum256(leafAfterRestart.Raw) != fingerprintBefore {
		t.Fatalf("certificate changed across disabled restart: err=%v", err)
	}
	if err := reopened.EnableSaved(); err != nil {
		t.Fatal(err)
	}
	status = reopened.Status()
	if !status.Enabled || !status.Listening || !status.RedirectActive || !status.RedirectHTTP {
		t.Fatalf("saved HTTPS settings did not reactivate: %+v", status)
	}
	leafAfterEnable, err := x509.ParseCertificate(reopened.cert.Certificate[0])
	if err != nil || sha256.Sum256(leafAfterEnable.Raw) != fingerprintBefore {
		t.Fatalf("reactivation did not reuse the valid certificate: err=%v", err)
	}
	redirect := httptest.NewRecorder()
	reopened.WrapHTTP(mux).ServeHTTP(redirect, httptest.NewRequest(http.MethodGet, "http://localhost/system?q=2", nil))
	if redirect.Code != http.StatusTemporaryRedirect || redirect.Header().Get("Location") != "https://localhost:19443/system?q=2" || redirect.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("HTTP redirect did not resume after activation: %d %q", redirect.Code, redirect.Header().Get("Location"))
	}
}

func TestUnreadyHTTPSNeverRedirectsAndUnconfiguredHostsAreEmptyArray(t *testing.T) {
	root := t.TempDir()
	m, err := Open(root, "127.0.0.1:0", 19444)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close(testContext(t))
	if err := m.SetHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })); err != nil {
		t.Fatal(err)
	}
	if got := m.Status().Hosts; got == nil || len(got) != 0 {
		t.Fatalf("unconfigured HTTPS hosts = %#v, want empty array", got)
	}
	m.mu.Lock()
	m.state.Enabled, m.state.Redirect, m.state.Hosts = true, true, []string{"localhost"}
	m.mu.Unlock()
	response := httptest.NewRecorder()
	m.WrapHTTP(http.NotFoundHandler()).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://localhost/system", nil))
	if response.Code != http.StatusNotFound || response.Header().Get("Location") != "" {
		t.Fatalf("unready HTTPS emitted redirect: %d %q", response.Code, response.Header().Get("Location"))
	}
}

func waitListenerClosed(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err != nil {
			return
		}
		_ = conn.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("HTTPS listener %s remained reachable after Disable", address)
}

func TestCustomCertificateIsValidatedAndNeverExposedByStatus(t *testing.T) {
	root := t.TempDir()
	m, err := Open(root, "127.0.0.1:0", 18443)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close(testContext(t))
	if err := m.SetHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})); err != nil {
		t.Fatal(err)
	}
	cert, key, err := generateCertificate([]string{"recorder.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.EnableCertificate([]string{"recorder.example.test"}, cert, key, false); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(m.Status())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "PRIVATE KEY") || strings.Contains(string(encoded), "private_key_pem") {
		t.Fatal("HTTPS status exposed private material")
	}
	if err := m.EnableCertificate([]string{"wrong.example.test"}, cert, key, false); err == nil {
		t.Fatal("certificate host mismatch accepted")
	}
	if err := m.EnableCertificate([]string{"recorder.example.test"}, cert, "not-a-key", false); err == nil {
		t.Fatal("mismatched certificate key accepted")
	}
}

func TestNormalizeHostsAcceptsIPv6SANs(t *testing.T) {
	hosts, err := normalizeHosts([]string{"2001:db8::1", "Recorder.Example.Test"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(hosts, ",") != "2001:db8::1,recorder.example.test" {
		t.Fatalf("normalized IPv6/SAN hosts = %v", hosts)
	}
}

func TestCustomCertificateDisableRestartAndReenableReusesCertificate(t *testing.T) {
	root := t.TempDir()
	m, err := Open(root, "127.0.0.1:0", 18444)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.SetHandler(m.WrapHTTP(http.NotFoundHandler())); err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM, err := generateCertificate([]string{"recorder.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.EnableCertificate([]string{"recorder.example.test"}, certPEM, keyPEM, true); err != nil {
		t.Fatal(err)
	}
	listenerAddress, listening := m.ListenerAddress()
	if !listening {
		t.Fatal("custom HTTPS listener did not start")
	}
	before := sha256.Sum256(m.cert.Certificate[0])
	if err := m.Disable(); err != nil {
		t.Fatal(err)
	}
	if !m.Status().Configured || m.Status().RedirectActive || !m.Status().RedirectHTTP {
		t.Fatalf("custom certificate configuration was lost: %+v", m.Status())
	}
	waitListenerClosed(t, listenerAddress)
	reopened, err := Open(root, "127.0.0.1:0", 18444)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(testContext(t))
	if err := reopened.SetHandler(reopened.WrapHTTP(http.NotFoundHandler())); err != nil {
		t.Fatal(err)
	}
	if err := reopened.EnableSaved(); err != nil {
		t.Fatal(err)
	}
	if got := sha256.Sum256(reopened.cert.Certificate[0]); got != before {
		t.Fatal("custom certificate was not reused after restart")
	}
	if !reopened.Status().RedirectActive {
		t.Fatalf("custom certificate redirect did not activate: %+v", reopened.Status())
	}
	data, err := os.ReadFile(filepath.Join(root, "security", "https.json"))
	if err != nil || !strings.Contains(string(data), "PRIVATE KEY") {
		t.Fatalf("protected persistence did not retain private key: err=%v", err)
	}
	statusJSON, _ := json.Marshal(reopened.Status())
	if strings.Contains(string(statusJSON), "PRIVATE KEY") || strings.Contains(string(statusJSON), keyPEM) {
		t.Fatal("status leaked persisted private key")
	}
}

func TestExpiredCustomCertificateCannotBeReenabledButConfigurationRemains(t *testing.T) {
	root := t.TempDir()
	certPEM, keyPEM, err := generateCertificateUntil([]string{"recorder.example.test"}, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	m, err := Open(root, "127.0.0.1:0", 18446)
	if err != nil {
		t.Fatal(err)
	}
	m.state = state{Version: 1, Redirect: true, Port: 18446, Hosts: []string{"recorder.example.test"}, Certificate: certPEM, PrivateKey: keyPEM}
	if err := m.persist(m.state); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(root, "127.0.0.1:0", 18446)
	if err != nil {
		t.Fatalf("disabled expired certificate should remain loadable for safe diagnosis: %v", err)
	}
	defer reopened.Close(testContext(t))
	if err := reopened.SetHandler(reopened.WrapHTTP(http.NotFoundHandler())); err != nil {
		t.Fatal(err)
	}
	if err := reopened.EnableSaved(); err == nil {
		t.Fatal("expired custom certificate unexpectedly re-enabled")
	}
	status := reopened.Status()
	if status.Enabled || !status.Configured || !status.RedirectHTTP || status.RedirectActive || status.ACME {
		t.Fatalf("failed reactivation changed saved configuration: %+v", status)
	}
}

func TestSavedSelfSignedCertificateUsesExistingRenewalPolicyWhenReenabled(t *testing.T) {
	root := t.TempDir()
	certPEM, keyPEM, err := generateCertificateUntil([]string{"localhost"}, time.Now().Add(10*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	m, err := Open(root, "127.0.0.1:0", 18447)
	if err != nil {
		t.Fatal(err)
	}
	m.state = state{Version: 1, Port: 18447, Hosts: []string{"localhost"}, Certificate: certPEM, PrivateKey: keyPEM, SelfSigned: true, Redirect: true}
	if err := m.persist(m.state); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(root, "127.0.0.1:0", 18447)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(testContext(t))
	if err := reopened.SetHandler(reopened.WrapHTTP(http.NotFoundHandler())); err != nil {
		t.Fatal(err)
	}
	before := sha256.Sum256(reopened.cert.Certificate[0])
	if err := reopened.EnableSaved(); err != nil {
		t.Fatal(err)
	}
	if got := sha256.Sum256(reopened.cert.Certificate[0]); got == before {
		t.Fatal("near-expiry self-signed certificate was not renewed on reactivation")
	}
	leaf, err := x509.ParseCertificate(reopened.cert.Certificate[0])
	if err != nil || time.Until(leaf.NotAfter) < 365*24*time.Hour {
		t.Fatalf("renewed self-signed certificate is not valid for the expected period: leaf=%v err=%v", leaf, err)
	}
}

func TestSelfSignedCertificateRenewsBeforeExpiryDuringServing(t *testing.T) {
	root := t.TempDir()
	m, err := Open(root, "127.0.0.1:0", 19445)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close(testContext(t))
	hosts := []string{"localhost"}
	certPEM, keyPEM, err := generateCertificateUntil(hosts, time.Now().Add(10*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	cert, err := parsePair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	m.state = state{Version: 1, Enabled: true, Port: m.publicPort, Hosts: hosts, Certificate: certPEM, PrivateKey: keyPEM, SelfSigned: true}
	m.cert = &cert
	if err := m.persist(m.state); err != nil {
		t.Fatal(err)
	}
	if err := m.renewSelfSignedIfDue(); err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(m.cert.Certificate[0])
	if err != nil || time.Until(leaf.NotAfter) < 365*24*time.Hour {
		t.Fatalf("self-signed certificate did not renew: expiry=%v err=%v", leaf.NotAfter, err)
	}
	reopened, err := Open(root, "127.0.0.1:0", m.publicPort)
	if err != nil || reopened.Status().ExpiresAt == "" {
		t.Fatalf("renewed certificate was not durably persisted: status=%+v err=%v", reopened.Status(), err)
	}
}

func TestACMEWaitsForARealCertificateBeforeRedirecting(t *testing.T) {
	root := t.TempDir()
	m, err := Open(root, "127.0.0.1:0", 19444)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close(testContext(t))
	wrapped := m.WrapHTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	if err := m.SetHandler(wrapped); err != nil {
		t.Fatal(err)
	}
	m.lookupHost = func(context.Context, string) ([]string, error) { return nil, os.ErrNotExist }
	if err := m.EnableACME("recorder.example.test", true); err == nil {
		t.Fatal("ACME accepted a domain that did not resolve during preflight")
	}
	m.lookupHost = func(context.Context, string) ([]string, error) { return []string{"192.168.1.20"}, nil }
	if err := m.EnableACME("recorder.example.test", true); err == nil {
		t.Fatal("ACME accepted a domain resolving only to a private address")
	}
	m.lookupHost = func(context.Context, string) ([]string, error) { return []string{"192.0.2.10"}, nil }
	if err := m.EnableACME("recorder.example.test", true); err == nil {
		t.Fatal("ACME accepted a non-443 public HTTPS port")
	}
	m.publicPort = 443
	if err := m.EnableACME("recorder.example.test", true); err != nil {
		t.Fatal(err)
	}
	status := m.Status()
	if !status.Enabled || !status.Listening || status.Ready || !status.ACME {
		t.Fatalf("ACME must be reported pending until a certificate is obtained: %+v", status)
	}
	response := httptest.NewRecorder()
	wrapped.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://recorder.example.test/", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("HTTP redirected before ACME certificate was ready: %d", response.Code)
	}
	if _, err := os.Stat(filepath.Join(root, "security", "acme")); !os.IsNotExist(err) {
		t.Fatalf("ACME cache was unexpectedly initialized before issuance: %v", err)
	}
	certPEM, keyPEM, err := generateCertificate([]string{"recorder.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	cacheValue := append([]byte(keyPEM), []byte(certPEM)...)
	if err := m.acme.Cache.Put(context.Background(), "recorder.example.test", cacheValue); err != nil {
		t.Fatal(err)
	}
	if !m.Status().Ready {
		t.Fatalf("valid cached certificate was not reported ready: %+v", m.Status())
	}
	response = httptest.NewRecorder()
	wrapped.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://recorder.example.test/", nil))
	if response.Code != http.StatusTemporaryRedirect || response.Header().Get("Location") != "https://recorder.example.test/" || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("ready ACME redirect = %d %q", response.Code, response.Header().Get("Location"))
	}
	listenerAddress, listening := m.ListenerAddress()
	if !listening {
		t.Fatal("ACME listener did not start")
	}
	cachedBefore, err := m.acme.Cache.Get(context.Background(), "recorder.example.test")
	if err != nil {
		t.Fatal(err)
	}
	certBefore, err := cachedACMECertificate(cachedBefore)
	if err != nil {
		t.Fatal(err)
	}
	fingerprintBefore := sha256.Sum256(certBefore.Certificate[0])
	if err := m.Disable(); err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	wrapped.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://recorder.example.test/", nil))
	if response.Code != http.StatusNoContent || response.Header().Get("Location") != "" {
		t.Fatalf("ACME redirect remained active while disabled: %d %q", response.Code, response.Header().Get("Location"))
	}
	waitListenerClosed(t, listenerAddress)
	reopened, err := Open(root, "127.0.0.1:0", 443)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(testContext(t))
	if err := reopened.SetHandler(reopened.WrapHTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))); err != nil {
		t.Fatal(err)
	}
	if err := reopened.EnableSaved(); err != nil {
		t.Fatal(err)
	}
	cachedAfter, err := reopened.acme.Cache.Get(context.Background(), "recorder.example.test")
	if err != nil {
		t.Fatal(err)
	}
	certAfter, err := cachedACMECertificate(cachedAfter)
	if err != nil || sha256.Sum256(certAfter.Certificate[0]) != fingerprintBefore {
		t.Fatalf("ACME reactivation did not reuse the cached certificate: %v", err)
	}
	status = reopened.Status()
	if !status.Enabled || !status.Ready || !status.RedirectActive || !status.ACME || !status.RedirectHTTP {
		t.Fatalf("cached ACME settings did not reactivate: %+v", status)
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}
