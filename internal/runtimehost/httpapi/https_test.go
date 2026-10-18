package httpapi

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/runtimehost/tlsserver"
)

type fakeHTTPSController struct {
	enabled  bool
	host     string
	key      string
	redirect bool
	ready    bool
}

func (f *fakeHTTPSController) Status() tlsserver.Status {
	return tlsserver.Status{Enabled: f.enabled, Ready: f.ready, Hosts: []string{f.host}}
}
func (f *fakeHTTPSController) EnableSelfSigned(hosts []string, redirect bool) error {
	f.enabled = true
	f.host = strings.Join(hosts, ",")
	f.redirect = redirect
	return nil
}
func (f *fakeHTTPSController) EnableACME(domain string, redirect bool) error {
	f.enabled = true
	f.host = domain
	f.redirect = redirect
	return nil
}
func (f *fakeHTTPSController) EnableCertificate(hosts []string, cert, key string, redirect bool) error {
	f.enabled = true
	f.host = strings.Join(hosts, ",")
	f.key = key
	f.redirect = redirect
	return nil
}
func (f *fakeHTTPSController) Disable() error { f.enabled = false; return nil }

func TestHTTPSSettingsAPIRequiresSessionAndCSRFAndUsesSecureCookieRefresh(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = auth.Bootstrap("owner", "correct horse battery"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.LoginAs("owner", "correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	controller := &fakeHTTPSController{ready: true}
	handler, err := WithHTTPS(auth, false, controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, HTTPSSettingsEndpoint, nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status code=%d", unauthorized.Code)
	}
	req := httptest.NewRequest(http.MethodPut, HTTPSSettingsEndpoint, strings.NewReader(`{"hosts":["another.example.test"],"redirect_http":true}`))
	req.Host = "recorder.example.test"
	req.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	req.AddCookie(&http.Cookie{Name: authn.CSRFCookieName, Value: session.CSRFToken})
	req.Header.Set("X-CSRF-Token", session.CSRFToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK || !controller.enabled || controller.host != "another.example.test,recorder.example.test" {
		t.Fatalf("HTTPS update code=%d controller=%+v body=%s", response.Code, controller, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "PRIVATE KEY") {
		t.Fatal("HTTPS status leaked private key")
	}
	secureCookies := 0
	for _, cookie := range response.Result().Cookies() {
		if cookie.Name == authn.SessionCookieName && cookie.Secure && cookie.HttpOnly && cookie.Value == session.Token {
			secureCookies++
		}
		if cookie.Name == authn.CSRFCookieName && cookie.Secure && !cookie.HttpOnly {
			secureCookies++
		}
	}
	if secureCookies != 2 {
		t.Fatalf("HTTPS activation did not upgrade both auth cookies: %+v", response.Result().Cookies())
	}
	bad := httptest.NewRecorder()
	badReq := httptest.NewRequest(http.MethodPut, HTTPSSettingsEndpoint, strings.NewReader(`{"hosts":["x"]}`))
	badReq.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	badReq.Header.Set("X-CSRF-Token", "wrong")
	handler.ServeHTTP(bad, badReq)
	if bad.Code != http.StatusForbidden {
		t.Fatalf("missing CSRF accepted: %d", bad.Code)
	}
}

func TestPendingACMEDoesNotUpgradeHTTPSessionCookie(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = auth.Bootstrap("owner", "correct horse battery"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.LoginAs("owner", "correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	controller := &fakeHTTPSController{}
	handler, err := WithHTTPS(auth, false, controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPut, HTTPSSettingsEndpoint, strings.NewReader(`{"hosts":["recorder.example.test"],"redirect_http":true,"acme":true}`))
	req.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	req.Header.Set("X-CSRF-Token", session.CSRFToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("pending ACME configuration failed: %d %s", response.Code, response.Body.String())
	}
	for _, cookie := range response.Result().Cookies() {
		if cookie.Name == authn.SessionCookieName && cookie.Secure {
			t.Fatal("pending ACME configuration stranded the HTTP session in a Secure cookie")
		}
	}
}

func TestPrivateKeyUploadRequiresHTTPS(t *testing.T) {
	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = auth.Bootstrap("owner", "correct horse battery"); err != nil {
		t.Fatal(err)
	}
	session, err := auth.LoginAs("owner", "correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	body := `{"hosts":["recorder.example.test"],"certificate_pem":"cert","private_key_pem":"private"}`
	controller := &fakeHTTPSController{}
	handler, err := WithHTTPS(auth, false, controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	remote := httptest.NewRequest(http.MethodPut, HTTPSSettingsEndpoint, strings.NewReader(body))
	remote.RemoteAddr = "192.0.2.10:43120"
	remote.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	remote.AddCookie(&http.Cookie{Name: authn.CSRFCookieName, Value: session.CSRFToken})
	remote.Header.Set("X-CSRF-Token", session.CSRFToken)
	remote.Header.Set("Origin", "http://recorder.example.test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, remote)
	if response.Code != http.StatusForbidden || controller.enabled {
		t.Fatalf("cleartext remote private-key upload status=%d enabled=%v body=%s", response.Code, controller.enabled, response.Body.String())
	}

	loopback := httptest.NewRequest(http.MethodPut, HTTPSSettingsEndpoint, strings.NewReader(body))
	loopback.RemoteAddr = "127.0.0.1:43120"
	loopback.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	loopback.AddCookie(&http.Cookie{Name: authn.CSRFCookieName, Value: session.CSRFToken})
	loopback.Header.Set("X-CSRF-Token", session.CSRFToken)
	loopbackResponse := httptest.NewRecorder()
	handler.ServeHTTP(loopbackResponse, loopback)
	if loopbackResponse.Code != http.StatusForbidden || controller.enabled {
		t.Fatalf("cleartext loopback private-key upload status=%d enabled=%v body=%s", loopbackResponse.Code, controller.enabled, loopbackResponse.Body.String())
	}

	secure := httptest.NewRequest(http.MethodPut, HTTPSSettingsEndpoint, strings.NewReader(body))
	secure.TLS = &tls.ConnectionState{}
	secure.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	secure.AddCookie(&http.Cookie{Name: authn.CSRFCookieName, Value: session.CSRFToken})
	secure.Header.Set("X-CSRF-Token", session.CSRFToken)
	secureResponse := httptest.NewRecorder()
	handler.ServeHTTP(secureResponse, secure)
	if secureResponse.Code != http.StatusOK || !controller.enabled {
		t.Fatalf("HTTPS private-key upload status=%d enabled=%v body=%s", secureResponse.Code, controller.enabled, secureResponse.Body.String())
	}
}
