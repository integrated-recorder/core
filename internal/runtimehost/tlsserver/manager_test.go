package tlsserver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
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
	redirect := httptest.NewRecorder()
	wrapped.ServeHTTP(redirect, httptest.NewRequest(http.MethodGet, "http://localhost/healthz?q=1", nil))
	if redirect.Code != http.StatusPermanentRedirect || redirect.Header().Get("Location") != "https://localhost:19443/healthz?q=1" {
		t.Fatalf("redirect = %d %q", redirect.Code, redirect.Header().Get("Location"))
	}
	if err := m.Disable(); err != nil {
		t.Fatal(err)
	}
	if m.Status().Enabled || m.Status().Listening {
		t.Fatalf("HTTPS remained enabled: %+v", m.Status())
	}
	if _, err := Open(root, "127.0.0.1:0", 19443); err != nil {
		t.Fatal(err)
	}
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
	if response.Code != http.StatusPermanentRedirect || response.Header().Get("Location") != "https://recorder.example.test/" {
		t.Fatalf("ready ACME redirect = %d %q", response.Code, response.Header().Get("Location"))
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}
