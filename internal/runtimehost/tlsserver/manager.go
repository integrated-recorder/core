// Package tlsserver provides Runtime Host's directly managed HTTPS listener.
// Certificate private keys are kept only in a 0700 directory and a 0600 file;
// public status never contains certificate or key bytes.
package tlsserver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/acme/autocert"
)

const (
	defaultPort = 8443
	maxState    = 2 << 20
)

type state struct {
	Version     int      `json:"version"`
	Enabled     bool     `json:"enabled"`
	Redirect    bool     `json:"redirect_http"`
	Port        int      `json:"port"`
	Hosts       []string `json:"hosts"`
	Certificate string   `json:"certificate_pem,omitempty"`
	PrivateKey  string   `json:"private_key_pem,omitempty"`
	SelfSigned  bool     `json:"self_signed"`
	ACME        bool     `json:"acme"`
}

type Status struct {
	Enabled        bool     `json:"enabled"`
	Configured     bool     `json:"configured"`
	Listening      bool     `json:"listening"`
	Ready          bool     `json:"certificate_ready"`
	RedirectHTTP   bool     `json:"redirect_http"`
	RedirectActive bool     `json:"redirect_active"`
	Port           int      `json:"port"`
	HTTPPort       int      `json:"http_port"`
	HTTPRecovery   bool     `json:"http_recovery_available"`
	Hosts          []string `json:"hosts"`
	SelfSigned     bool     `json:"self_signed"`
	ACME           bool     `json:"acme"`
	IssuanceFailed bool     `json:"issuance_failed"`
	ExpiresAt      string   `json:"expires_at,omitempty"`
}

type Manager struct {
	mu         sync.Mutex
	path       string
	listen     string
	publicPort int
	httpPort   int
	state      state
	cert       *tls.Certificate
	acme       *autocert.Manager
	handler    http.Handler
	listener   net.Listener
	server     *http.Server
	stopDone   chan struct{}
	acmeFailed atomic.Bool
	lookupHost func(context.Context, string) ([]string, error)
}

func Open(dataRoot, listenAddress string, publicPort int) (*Manager, error) {
	if !filepath.IsAbs(dataRoot) || filepath.Clean(dataRoot) != dataRoot || listenAddress == "" || publicPort < 1 || publicPort > 65535 {
		return nil, errors.New("HTTPS storage or listener configuration is invalid")
	}
	dir := filepath.Join(dataRoot, "security")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, errors.New("HTTPS security storage is unavailable")
	}
	dirInfo, err := os.Lstat(dir)
	if err != nil || !dirInfo.IsDir() || dirInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("HTTPS security storage is unavailable")
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, errors.New("HTTPS security storage could not be protected")
	}
	m := &Manager{path: filepath.Join(dir, "https.json"), listen: listenAddress, publicPort: publicPort, httpPort: 8080, state: state{Version: 1, Port: publicPort}, lookupHost: net.DefaultResolver.LookupHost}
	info, err := os.Lstat(m.path)
	if errors.Is(err, os.ErrNotExist) {
		return m, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 0 || info.Size() > maxState {
		return nil, errors.New("HTTPS configuration is unavailable")
	}
	b, err := os.ReadFile(m.path)
	if err != nil || len(b) > maxState {
		return nil, errors.New("HTTPS configuration is unavailable")
	}
	var loaded state
	if json.Unmarshal(b, &loaded) != nil || loaded.Version != 1 || loaded.Port < 1 || loaded.Port > 65535 {
		return nil, errors.New("HTTPS configuration is invalid")
	}
	m.state = loaded
	configured := httpsConfigured(loaded)
	if loaded.Enabled || configured {
		hosts, hostErr := normalizeHosts(loaded.Hosts)
		if hostErr != nil {
			return nil, errors.New("persisted HTTPS hosts are invalid")
		}
		loaded.Hosts = hosts
		m.state = loaded
		if loaded.ACME {
			if loaded.SelfSigned || loaded.Certificate != "" || loaded.PrivateKey != "" || len(hosts) != 1 || net.ParseIP(hosts[0]) != nil || !strings.Contains(hosts[0], ".") {
				return nil, errors.New("persisted ACME configuration is invalid")
			}
		} else {
			cert, certErr := parsePair(loaded.Certificate, loaded.PrivateKey)
			if certErr != nil || len(cert.Certificate) == 0 {
				return nil, errors.New("persisted HTTPS certificate is invalid")
			}
			leaf, parseErr := x509.ParseCertificate(cert.Certificate[0])
			if parseErr != nil || time.Now().Before(leaf.NotBefore) || (loaded.Enabled && !loaded.SelfSigned && time.Now().After(leaf.NotAfter)) {
				return nil, errors.New("persisted HTTPS certificate is invalid")
			}
			for _, host := range hosts {
				if leaf.VerifyHostname(host) != nil {
					return nil, errors.New("persisted HTTPS certificate does not cover configured hosts")
				}
			}
			m.cert = &cert
		}
	}
	// The host-side HTTPS port is part of the installer-managed Compose
	// mapping. Updating it is a deliberate installation setting change and does
	// not alter application data or release identity.
	if loaded.Port != publicPort {
		m.state.Port = publicPort
		if err := m.persist(m.state); err != nil {
			return nil, errors.New("HTTPS public port could not be updated")
		}
	}
	if configured {
		if loaded.ACME {
			m.acme = m.newACME(loaded.Hosts[0])
		} else {
			// The certificate was parsed and checked against every configured SAN above.
		}
		if loaded.Enabled && loaded.SelfSigned {
			cert := *m.cert
			leaf, parseErr := x509.ParseCertificate(cert.Certificate[0])
			if parseErr != nil {
				return nil, errors.New("persisted HTTPS certificate is invalid")
			}
			if time.Until(leaf.NotAfter) <= 30*24*time.Hour {
				certPEM, keyPEM, genErr := generateCertificate(loaded.Hosts)
				if genErr != nil {
					return nil, errors.New("self-signed HTTPS certificate could not be renewed")
				}
				loaded.Certificate, loaded.PrivateKey = certPEM, keyPEM
				m.state = loaded
				cert, _ = parsePair(certPEM, keyPEM)
				m.cert = &cert
				if err := m.persist(loaded); err != nil {
					return nil, errors.New("renewed HTTPS certificate could not be persisted")
				}
			}
		}
	}
	if err := os.Chmod(m.path, 0600); err != nil {
		return nil, errors.New("HTTPS configuration could not be protected")
	}
	return m, nil
}

func (m *Manager) SetHandler(handler http.Handler) error {
	if handler == nil {
		return errors.New("HTTPS handler is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handler = handler
	return m.syncLocked(context.Background())
}

// SetHTTPPublicPort records the installer-published HTTP port so the browser
// can return to the authenticated cleartext UI when HTTPS is intentionally
// disabled. The application listener itself remains installer-configured.
func (m *Manager) SetHTTPPublicPort(port int) error {
	if port < 1 || port > 65535 {
		return errors.New("HTTP public port is invalid")
	}
	m.mu.Lock()
	m.httpPort = port
	m.mu.Unlock()
	return nil
}

func (m *Manager) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := Status{Enabled: m.state.Enabled, Configured: httpsConfigured(m.state), Listening: m.server != nil, RedirectHTTP: m.state.Redirect, Port: m.state.Port, HTTPPort: m.httpPort, HTTPRecovery: true, Hosts: append([]string(nil), m.state.Hosts...), SelfSigned: m.state.SelfSigned, ACME: m.state.ACME}
	if m.cert != nil && len(m.cert.Certificate) > 0 {
		if leaf, err := x509.ParseCertificate(m.cert.Certificate[0]); err == nil {
			out.ExpiresAt = leaf.NotAfter.UTC().Format(time.RFC3339)
			out.Ready = out.Listening && time.Now().Before(leaf.NotAfter)
		}
	}
	if m.acme != nil && len(m.state.Hosts) == 1 {
		if data, err := m.acme.Cache.Get(context.Background(), m.state.Hosts[0]); err == nil {
			if cert, err := cachedACMECertificate(data); err == nil && len(cert.Certificate) > 0 {
				if leaf, err := x509.ParseCertificate(cert.Certificate[0]); err == nil && leaf.VerifyHostname(m.state.Hosts[0]) == nil {
					out.ExpiresAt = leaf.NotAfter.UTC().Format(time.RFC3339)
					out.Ready = out.Listening && time.Now().Before(leaf.NotAfter)
				}
			}
		}
		out.IssuanceFailed = m.acmeFailed.Load()
	}
	out.RedirectActive = out.Enabled && out.Listening && out.RedirectHTTP && out.Ready
	return out
}

func httpsConfigured(config state) bool {
	return len(config.Hosts) > 0 && (config.ACME || config.Certificate != "" || config.PrivateKey != "")
}

func cachedACMECertificate(data []byte) (tls.Certificate, error) {
	key, rest := pem.Decode(data)
	if key == nil || !strings.Contains(key.Type, "PRIVATE KEY") {
		return tls.Certificate{}, errors.New("cached ACME key is invalid")
	}
	var certPEM []byte
	for len(rest) > 0 {
		block, next := pem.Decode(rest)
		if block == nil {
			break
		}
		rest = next
		if block.Type == "CERTIFICATE" {
			certPEM = append(certPEM, pem.EncodeToMemory(block)...)
		}
	}
	return tls.X509KeyPair(certPEM, pem.EncodeToMemory(key))
}

func (m *Manager) EnableSelfSigned(hosts []string, redirect bool) error {
	all, err := normalizeHosts(hosts)
	if err != nil {
		return err
	}
	all = appendInterfaceIPs(all)
	certPEM, keyPEM, err := generateCertificate(all)
	if err != nil {
		return errors.New("self-signed certificate could not be generated")
	}
	return m.update(state{Version: 1, Enabled: true, Redirect: redirect, Port: m.publicPort, Hosts: all, Certificate: certPEM, PrivateKey: keyPEM, SelfSigned: true})
}

func (m *Manager) EnableACME(domain string, redirect bool) error {
	if m.publicPort != 443 {
		return errors.New("ACME certificates require the public HTTPS port 443")
	}
	all, err := normalizeHosts([]string{domain})
	if err != nil {
		return err
	}
	if net.ParseIP(all[0]) != nil || !strings.Contains(all[0], ".") {
		return errors.New("ACME certificates require a public DNS name")
	}
	lookup := m.lookupHost
	if lookup == nil {
		lookup = net.DefaultResolver.LookupHost
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	addresses, err := lookup(ctx, all[0])
	publicAddress := false
	for _, address := range addresses {
		ip := net.ParseIP(address)
		if ip != nil && ip.IsGlobalUnicast() && !ip.IsPrivate() {
			publicAddress = true
			break
		}
	}
	if err != nil || !publicAddress {
		return errors.New("ACME domain must resolve to a public address")
	}
	return m.update(state{Version: 1, Enabled: true, Redirect: redirect, Port: m.publicPort, Hosts: all, ACME: true})
}

func (m *Manager) EnableCertificate(hosts []string, certPEM, keyPEM string, redirect bool) error {
	all, err := normalizeHosts(hosts)
	if err != nil {
		return err
	}
	cert, err := parsePair(certPEM, keyPEM)
	if err != nil {
		return errors.New("certificate and private key are invalid or do not match")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil || time.Now().Before(leaf.NotBefore) || time.Now().After(leaf.NotAfter) {
		return errors.New("certificate is not currently valid")
	}
	for _, host := range all {
		if leaf.VerifyHostname(host) != nil {
			return errors.New("certificate does not cover every configured host")
		}
	}
	return m.update(state{Version: 1, Enabled: true, Redirect: redirect, Port: m.publicPort, Hosts: all, Certificate: certPEM, PrivateKey: keyPEM})
}

func (m *Manager) Disable() error {
	m.mu.Lock()
	next := m.state
	next.Enabled = false
	if err := m.persist(next); err != nil {
		m.mu.Unlock()
		return err
	}
	m.state = next
	server := m.server
	m.server, m.listener = nil, nil
	if server != nil {
		done := make(chan struct{})
		m.stopDone = done
	}
	done := m.stopDone
	m.mu.Unlock()
	if server != nil {
		// Stop accepting new TLS requests immediately while allowing the current
		// authenticated disable response to finish before the browser moves to HTTP.
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := server.Shutdown(ctx); err != nil {
				_ = server.Close()
			}
			m.mu.Lock()
			if m.stopDone == done {
				m.stopDone = nil
			}
			close(done)
			m.mu.Unlock()
		}()
	}
	return nil
}

// EnableSaved reactivates the last explicitly configured TLS mode without
// changing its hosts, redirect preference, certificate, or key. Self-signed
// certificates use the existing 30-day renewal policy; custom certificates
// must still be currently valid. ACME uses its protected persistent cache.
func (m *Manager) EnableSaved() error {
	for {
		m.mu.Lock()
		stopping := m.stopDone
		if stopping == nil {
			break
		}
		m.mu.Unlock()
		<-stopping
	}
	defer m.mu.Unlock()
	previous := m.state
	if !httpsConfigured(previous) {
		return errors.New("no saved HTTPS configuration is available")
	}
	next := previous
	next.Enabled = true
	var nextCert *tls.Certificate
	var nextACME *autocert.Manager
	if next.ACME {
		if len(next.Hosts) != 1 || net.ParseIP(next.Hosts[0]) != nil || !strings.Contains(next.Hosts[0], ".") {
			return errors.New("saved ACME configuration is invalid")
		}
		nextACME = m.acme
		if nextACME == nil {
			nextACME = m.newACME(next.Hosts[0])
		}
	} else {
		cert, err := parsePair(next.Certificate, next.PrivateKey)
		if err != nil || len(cert.Certificate) == 0 {
			return errors.New("saved HTTPS certificate is invalid")
		}
		leaf, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil || time.Now().Before(leaf.NotBefore) {
			return errors.New("saved HTTPS certificate is not currently valid")
		}
		for _, host := range next.Hosts {
			if leaf.VerifyHostname(host) != nil {
				return errors.New("saved HTTPS certificate does not cover configured hosts")
			}
		}
		if time.Now().After(leaf.NotAfter) || (next.SelfSigned && time.Until(leaf.NotAfter) <= 30*24*time.Hour) {
			if !next.SelfSigned {
				return errors.New("saved HTTPS certificate has expired")
			}
			certPEM, keyPEM, err := generateCertificate(next.Hosts)
			if err != nil {
				return errors.New("self-signed HTTPS certificate could not be renewed")
			}
			next.Certificate, next.PrivateKey = certPEM, keyPEM
			cert, err = parsePair(certPEM, keyPEM)
			if err != nil {
				return errors.New("self-signed HTTPS certificate could not be renewed")
			}
		}
		nextCert = &cert
	}
	if err := m.persist(next); err != nil {
		return err
	}
	previousCert, previousACME := m.cert, m.acme
	m.state, m.cert, m.acme = next, nextCert, nextACME
	m.acmeFailed.Store(false)
	if err := m.syncLocked(context.Background()); err != nil {
		m.state, m.cert, m.acme = previous, previousCert, previousACME
		_ = m.persist(previous)
		return err
	}
	return nil
}

func (m *Manager) update(next state) error {
	cert, err := parsePair(next.Certificate, next.PrivateKey)
	if err != nil && !next.ACME {
		return errors.New("HTTPS certificate is invalid")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	previous, previousCert, previousACME := m.state, m.cert, m.acme
	if err := m.persist(next); err != nil {
		return err
	}
	m.state = next
	m.cert = nil
	m.acme = nil
	m.acmeFailed.Store(false)
	if next.ACME {
		m.acme = m.newACME(next.Hosts[0])
	} else {
		m.cert = &cert
	}
	if err := m.syncLocked(context.Background()); err != nil {
		m.state, m.cert, m.acme = previous, previousCert, previousACME
		_ = m.persist(previous)
		return err
	}
	return nil
}

func (m *Manager) syncLocked(ctx context.Context) error {
	if !m.state.Enabled || m.handler == nil {
		if m.server != nil {
			server := m.server
			m.server = nil
			m.listener = nil
			go func() { _ = server.Shutdown(ctx) }()
		}
		return nil
	}
	if m.server != nil {
		return nil
	}
	listener, err := net.Listen("tcp", m.listen)
	if err != nil {
		return errors.New("HTTPS listener could not be opened; check the configured port")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1", "acme-tls/1"}, GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		if err := m.renewSelfSignedIfDue(); err != nil {
			return nil, err
		}
		m.mu.Lock()
		acme, cert := m.acme, m.cert
		m.mu.Unlock()
		if acme != nil {
			certificate, err := acme.GetCertificate(hello)
			m.acmeFailed.Store(err != nil)
			return certificate, err
		}
		if cert == nil {
			return nil, errors.New("HTTPS certificate is unavailable")
		}
		return cert, nil
	}}
	server := &http.Server{Handler: m.handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second, TLSConfig: tlsConfig}
	m.listener = listener
	m.server = server
	go func() {
		_ = server.Serve(tls.NewListener(listener, tlsConfig))
		m.mu.Lock()
		if m.server == server {
			m.server = nil
			m.listener = nil
		}
		m.mu.Unlock()
	}()
	return nil
}

func (m *Manager) renewSelfSignedIfDue() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.state.Enabled || !m.state.SelfSigned || m.cert == nil || len(m.cert.Certificate) == 0 {
		return nil
	}
	leaf, err := x509.ParseCertificate(m.cert.Certificate[0])
	if err != nil {
		return errors.New("HTTPS certificate is invalid")
	}
	if time.Until(leaf.NotAfter) > 30*24*time.Hour {
		return nil
	}
	certPEM, keyPEM, err := generateCertificate(m.state.Hosts)
	if err != nil {
		return errors.New("HTTPS certificate renewal failed")
	}
	next := m.state
	next.Certificate, next.PrivateKey = certPEM, keyPEM
	cert, err := parsePair(certPEM, keyPEM)
	if err != nil {
		return errors.New("HTTPS certificate renewal failed")
	}
	if err := m.persist(next); err != nil {
		return errors.New("HTTPS certificate renewal could not be persisted")
	}
	m.state, m.cert = next, &cert
	return nil
}

func (m *Manager) WrapHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		redirect, enabled, port, acme := m.state.Redirect, m.state.Enabled, m.state.Port, m.acme
		hosts := append([]string(nil), m.state.Hosts...)
		m.mu.Unlock()
		if r.TLS == nil && enabled && acme != nil && strings.HasPrefix(r.URL.Path, "/.well-known/acme-challenge/") {
			acme.HTTPHandler(next).ServeHTTP(w, r)
			return
		}
		if r.TLS == nil && enabled && redirect && m.Status().Ready {
			host := requestHostName(r.Host)
			found := false
			for _, configured := range hosts {
				if strings.EqualFold(configured, host) {
					found = true
					break
				}
			}
			if !found && len(hosts) > 0 {
				host = hosts[0]
			}
			if ip := net.ParseIP(host); ip != nil && ip.To4() == nil {
				host = "[" + host + "]"
			}
			if port != 443 {
				host = net.JoinHostPort(host, fmt.Sprint(port))
			}
			http.Redirect(w, r, "https://"+host+r.URL.RequestURI(), http.StatusPermanentRedirect)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (m *Manager) newACME(domain string) *autocert.Manager {
	cache := filepath.Join(filepath.Dir(m.path), "acme")
	return &autocert.Manager{Prompt: autocert.AcceptTOS, HostPolicy: autocert.HostWhitelist(domain), Cache: autocert.DirCache(cache)}
}

func requestHostName(value string) string {
	if host, _, err := net.SplitHostPort(value); err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.Trim(value, "[]")
}

func (m *Manager) isListening() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.server != nil }

// ListenerAddress reports the bound address without exposing the listener or
// certificate. It is primarily useful to deterministic process/browser tests.
func (m *Manager) ListenerAddress() (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listener == nil {
		return "", false
	}
	return m.listener.Addr().String(), true
}

func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	server := m.server
	stopping := m.stopDone
	m.server = nil
	m.listener = nil
	m.mu.Unlock()
	if server == nil {
		if stopping != nil {
			select {
			case <-stopping:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	if err := server.Shutdown(ctx); err != nil {
		_ = server.Close()
		return err
	}
	return nil
}

func (m *Manager) persist(next state) error {
	b, err := json.Marshal(next)
	if err != nil {
		return errors.New("HTTPS configuration could not be encoded")
	}
	tmp, err := os.CreateTemp(filepath.Dir(m.path), ".https-*.tmp")
	if err != nil {
		return errors.New("HTTPS configuration could not be written")
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err = tmp.Chmod(0600); err == nil {
		_, err = tmp.Write(b)
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return errors.New("HTTPS configuration could not be persisted")
	}
	if err = os.Rename(name, m.path); err != nil {
		return errors.New("HTTPS configuration could not be published")
	}
	dir, err := os.Open(filepath.Dir(m.path))
	if err != nil {
		return errors.New("HTTPS configuration directory could not be synced")
	}
	defer dir.Close()
	if err = dir.Sync(); err != nil {
		return errors.New("HTTPS configuration directory could not be synced")
	}
	return nil
}

func normalizeHosts(hosts []string) ([]string, error) {
	if len(hosts) == 0 || len(hosts) > 32 {
		return nil, errors.New("at least one certificate host is required")
	}
	set := map[string]bool{}
	for _, host := range hosts {
		host = strings.TrimSpace(strings.TrimSuffix(host, "."))
		if host == "" || strings.ContainsAny(host, "/@\\ \t\r\n") || len(host) > 253 {
			return nil, errors.New("certificate host is invalid")
		}
		if net.ParseIP(host) == nil {
			if strings.Contains(host, ":") {
				return nil, errors.New("certificate host is invalid")
			}
			for _, label := range strings.Split(host, ".") {
				if label == "" || len(label) > 63 {
					return nil, errors.New("certificate host is invalid")
				}
				for _, r := range label {
					if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
						return nil, errors.New("certificate host is invalid")
					}
				}
			}
		}
		set[strings.ToLower(host)] = true
	}
	out := make([]string, 0, len(set))
	for host := range set {
		out = append(out, host)
	}
	sort.Strings(out)
	return out, nil
}

func appendInterfaceIPs(hosts []string) []string {
	set := map[string]bool{}
	for _, h := range hosts {
		set[h] = true
	}
	ifs, _ := net.InterfaceAddrs()
	for _, a := range ifs {
		ip, _, err := net.ParseCIDR(a.String())
		if err == nil && !ip.IsLoopback() {
			set[ip.String()] = true
		}
	}
	out := make([]string, 0, len(set))
	for h := range set {
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

func generateCertificate(hosts []string) (string, string, error) {
	return generateCertificateUntil(hosts, time.Now().AddDate(2, 0, 0))
}

func generateCertificateUntil(hosts []string, expires time.Time) (string, string, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return "", "", err
	}
	template := x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "Integrated Recorder HTTPS"}, NotBefore: time.Now().Add(-5 * time.Minute), NotAfter: expires, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return "", "", err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", "", err
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})), nil
}

func parsePair(certPEM, keyPEM string) (tls.Certificate, error) {
	if len(certPEM) > 1<<20 || len(keyPEM) > 1<<20 {
		return tls.Certificate{}, errors.New("certificate material is too large")
	}
	return tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
}
