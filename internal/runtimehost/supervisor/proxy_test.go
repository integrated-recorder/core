package supervisor

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestFixedProxyPreservesBrowserHostAndStripsUntrustedForwardedHeaders(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Observed-Host", r.Host)
		w.Header().Set("X-Observed-Forwarded-Host", r.Header.Get("X-Forwarded-Host"))
		w.Header().Set("X-Observed-IR-HTTPS", r.Header.Get("X-IR-HTTPS"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()
	target, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := makeProxy(target)
	request := httptest.NewRequest(http.MethodGet, "http://recorder.example.test:8088/api/auth/bootstrap", nil)
	request.Header.Set("X-Forwarded-Host", "attacker.example")
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("proxied status=%d", response.Code)
	}
	if got, want := response.Header().Get("X-Observed-Host"), "recorder.example.test:8088"; got != want {
		t.Fatalf("backend Host=%q, want original browser authority %q", got, want)
	}
	if got := response.Header().Get("X-Observed-Forwarded-Host"); got != "" {
		t.Fatalf("backend received untrusted forwarded Host %q", got)
	}
	if got := response.Header().Get("X-Observed-IR-HTTPS"); got != "" {
		t.Fatalf("HTTP request retained spoofed TLS marker %q", got)
	}

	secure := httptest.NewRequest(http.MethodGet, "https://recorder.example.test:8443/api/auth/session", nil)
	secure.TLS = &tls.ConnectionState{}
	secure.Header.Set("X-IR-HTTPS", "forged")
	secureResponse := httptest.NewRecorder()
	proxy.ServeHTTP(secureResponse, secure)
	if got := secureResponse.Header().Get("X-Observed-IR-HTTPS"); got != "1" {
		t.Fatalf("trusted TLS request marker=%q, want 1", got)
	}
}
