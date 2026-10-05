package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/plugintrust"
)

func TestAdapterAPIUsesHostPinnedTrustProjection(t *testing.T) {
	dir := t.TempDir()
	writeServerTestAdapter(t, dir)
	host, err := adapterhost.Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	bundled := plugintrust.NewBundled()
	handler := NewWithOptions(nil, host, nil, Options{AdapterTrust: map[string]plugintrust.Attestation{"schema-test": bundled}})
	for _, test := range []struct {
		name   string
		method string
		path   string
	}{
		{name: "list", method: http.MethodGet, path: "/api/adapters"},
		{name: "get", method: http.MethodGet, path: "/api/adapters/schema-test"},
		{name: "restart", method: http.MethodPost, path: "/api/adapters/schema-test/restart"},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(test.method, test.path, strings.NewReader("")))
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"provenance":"bundled"`) ||
				!strings.Contains(response.Body.String(), `"authority":"core_release"`) || !strings.Contains(response.Body.String(), `"publisher":"first_party"`) || !strings.Contains(response.Body.String(), `"reviewed":true`) {
				t.Fatalf("%s did not return Host-pinned trust: status=%d body=%s", test.name, response.Code, response.Body.String())
			}
		})
	}
}

func TestAdapterAPITrustProjectionIsConservativeForLegacyAndMonolithicControl(t *testing.T) {
	dir := t.TempDir()
	writeServerTestAdapter(t, dir)
	host, err := adapterhost.Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	legacy := NewWithOptions(nil, host, nil, Options{AdapterTrust: map[string]plugintrust.Attestation{"schema-test": plugintrust.Legacy()}})
	response := httptest.NewRecorder()
	legacy.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/adapters/schema-test", nil))
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), `"trust"`) {
		t.Fatalf("legacy pinned entry should remain unclassified: status=%d body=%s", response.Code, response.Body.String())
	}

	monolithic := New(nil, host, nil)
	response = httptest.NewRecorder()
	monolithic.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/adapters/schema-test", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"provenance":"operator"`) ||
		!strings.Contains(response.Body.String(), `"authority":"local"`) || !strings.Contains(response.Body.String(), `"publisher":"unknown"`) || strings.Contains(response.Body.String(), `"reviewed":true`) {
		t.Fatalf("monolithic adapter projection was not operator-equivalent: status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestAdapterBrandingAPIUsesAuthenticatedIconProjection(t *testing.T) {
	dir := t.TempDir()
	icon := serverTestPNG(t)
	writeServerBrandedTestAdapter(t, dir, icon)
	host, err := adapterhost.Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	products, err := management.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, host, nil, Options{Management: products})
	rawIcon := base64.StdEncoding.EncodeToString(icon)
	iconURL := `"icon_url":"/api/adapters/schema-test/icon"`
	assertSanitized := func(name string, response *httptest.ResponseRecorder) {
		t.Helper()
		body := response.Body.String()
		if response.Code != http.StatusOK || !strings.Contains(body, iconURL) || strings.Contains(body, rawIcon) || strings.Contains(body, `"data"`) {
			t.Fatalf("%s response was not sanitized: status=%d body=%s", name, response.Code, body)
		}
	}

	list := httptest.NewRecorder()
	handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/api/adapters", nil))
	assertSanitized("list", list)
	get := httptest.NewRecorder()
	handler.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/adapters/schema-test", nil))
	assertSanitized("get", get)
	for _, action := range []string{"restart", "disable", "enable"} {
		request := httptest.NewRequest(http.MethodPost, "/api/adapters/schema-test/"+action, strings.NewReader(""))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		assertSanitized(action, response)
	}

	iconResponse := httptest.NewRecorder()
	handler.ServeHTTP(iconResponse, httptest.NewRequest(http.MethodGet, "/api/adapters/schema-test/icon", nil))
	if iconResponse.Code != http.StatusOK || iconResponse.Header().Get("Content-Type") != "image/png" || iconResponse.Header().Get("X-Content-Type-Options") != "nosniff" || !strings.HasPrefix(iconResponse.Header().Get("Cache-Control"), "private,") || !bytes.Equal(iconResponse.Body.Bytes(), icon) {
		t.Fatalf("icon response status=%d headers=%v body=%x", iconResponse.Code, iconResponse.Header(), iconResponse.Body.Bytes())
	}
	etag := iconResponse.Header().Get("ETag")
	if !strings.HasPrefix(etag, `"`) || etag == `""` {
		t.Fatalf("invalid icon ETag %q", etag)
	}
	conditional := httptest.NewRequest(http.MethodGet, "/api/adapters/schema-test/icon", nil)
	conditional.Header.Set("If-None-Match", etag)
	notModified := httptest.NewRecorder()
	handler.ServeHTTP(notModified, conditional)
	if notModified.Code != http.StatusNotModified || notModified.Body.Len() != 0 {
		t.Fatalf("conditional icon response=%d body=%q", notModified.Code, notModified.Body.String())
	}

	missing := httptest.NewRecorder()
	handler.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/api/adapters/unknown/icon", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing icon status=%d body=%s", missing.Code, missing.Body.String())
	}

	root := t.TempDir()
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := os.ReadFile(filepath.Join(root, "security", "bootstrap-token"))
	if err != nil {
		t.Fatal(err)
	}
	const password = "correct horse battery staple"
	if err := auth.Bootstrap(strings.TrimSpace(string(bootstrap)), password); err != nil {
		t.Fatal(err)
	}
	authenticatedHandler := NewWithOptions(nil, host, nil, Options{Auth: auth})
	unauthorized := httptest.NewRecorder()
	authenticatedHandler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/adapters/schema-test/icon", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated icon request status=%d", unauthorized.Code)
	}
	session, err := auth.Login(password)
	if err != nil {
		t.Fatal(err)
	}
	authorizedRequest := httptest.NewRequest(http.MethodGet, "/api/adapters/schema-test/icon", nil)
	authorizedRequest.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
	authorized := httptest.NewRecorder()
	authenticatedHandler.ServeHTTP(authorized, authorizedRequest)
	if authorized.Code != http.StatusOK || !bytes.Equal(authorized.Body.Bytes(), icon) {
		t.Fatalf("authenticated icon request status=%d body=%x", authorized.Code, authorized.Body.Bytes())
	}
}

func TestAdapterIconWithoutBrandingReturnsNotFound(t *testing.T) {
	dir := t.TempDir()
	writeServerTestAdapter(t, dir)
	host, err := adapterhost.Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	response := httptest.NewRecorder()
	New(nil, host, nil).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/adapters/schema-test/icon", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("unbranded icon status=%d body=%s", response.Code, response.Body.String())
	}
}

func serverTestPNG(t *testing.T) []byte {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, 3, 2))
	img.SetNRGBA(0, 0, color.NRGBA{R: 50, G: 100, B: 150, A: 255})
	var output bytes.Buffer
	if err := png.Encode(&output, img); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func writeServerBrandedTestAdapter(t *testing.T, dir string, icon []byte) {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(icon)
	script := "#!/bin/sh\nIR_SERVER_ADAPTER_ICON=" + encoded + " IR_SERVER_ADAPTER_HELPER=1 exec '" + strings.ReplaceAll(binary, "'", "'\\''") + "'\n"
	if err := os.WriteFile(filepath.Join(dir, "integrated-recorder-adapter-schema-test"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
}

func TestAdapterControlRoutesPersistAndReturnRuntimeState(t *testing.T) {
	dir := t.TempDir()
	writeServerTestAdapter(t, dir)
	host, err := adapterhost.Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	productRoot := t.TempDir()
	products, err := management.Open(productRoot)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{adapters: host, products: products, mux: http.NewServeMux()}
	s.registerAdapterControlRoutes()
	handler := s.mux

	disabled := httptest.NewRecorder()
	handler.ServeHTTP(disabled, httptest.NewRequest(http.MethodPost, "/api/adapters/schema-test/disable", strings.NewReader("")))
	if disabled.Code != http.StatusOK || disabled.Header().Get("X-Adapter-Audit") != "recorded" || !strings.Contains(disabled.Body.String(), `"state":"disabled"`) || products.AdapterEnabled("schema-test") {
		t.Fatalf("disable response=%d %s, stored_enabled=%v", disabled.Code, disabled.Body.String(), products.AdapterEnabled("schema-test"))
	}

	enabled := httptest.NewRecorder()
	handler.ServeHTTP(enabled, httptest.NewRequest(http.MethodPost, "/api/adapters/schema-test/enable", strings.NewReader("")))
	if enabled.Code != http.StatusOK || enabled.Header().Get("X-Adapter-Audit") != "recorded" || !strings.Contains(enabled.Body.String(), `"state":"ready"`) || !products.AdapterEnabled("schema-test") {
		t.Fatalf("enable response=%d %s, stored_enabled=%v", enabled.Code, enabled.Body.String(), products.AdapterEnabled("schema-test"))
	}

	restarted := httptest.NewRecorder()
	handler.ServeHTTP(restarted, httptest.NewRequest(http.MethodPost, "/api/adapters/schema-test/restart", strings.NewReader("")))
	if restarted.Code != http.StatusOK || restarted.Header().Get("X-Adapter-Audit") != "recorded" || !strings.Contains(restarted.Body.String(), `"state":"ready"`) || !strings.Contains(restarted.Body.String(), `"generation":3`) {
		t.Fatalf("restart response=%d %s", restarted.Code, restarted.Body.String())
	}

	audit := products.Audit(10)
	if len(audit) != 3 || audit[0].Type != "adapter_restarted" || audit[1].Type != "adapter_enabled" || audit[2].Type != "adapter_disabled" {
		t.Fatalf("adapter audit=%#v", audit)
	}
}

func TestAdapterControlReturnsAppliedStateWhenAuditWriteFails(t *testing.T) {
	dir := t.TempDir()
	writeServerTestAdapter(t, dir)
	host, err := adapterhost.Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	productRoot := t.TempDir()
	products, err := management.Open(productRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(productRoot, "management", "audit.json"), 0700); err != nil {
		t.Fatal(err)
	}
	s := &Server{adapters: host, products: products, mux: http.NewServeMux()}
	s.registerAdapterControlRoutes()
	response := httptest.NewRecorder()
	s.mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/adapters/schema-test/disable", strings.NewReader("")))
	if response.Code != http.StatusOK || response.Header().Get("X-Adapter-Audit") != "failed" || !strings.Contains(response.Body.String(), `"state":"disabled"`) {
		t.Fatalf("applied state response=%d audit=%q body=%s", response.Code, response.Header().Get("X-Adapter-Audit"), response.Body.String())
	}
	if products.AdapterEnabled("schema-test") {
		t.Fatal("durable adapter preference did not reflect applied runtime state")
	}
}

func TestAdapterControlRoutesRejectUnknownAdapter(t *testing.T) {
	products, err := management.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{products: products, mux: http.NewServeMux()}
	s.registerAdapterControlRoutes()
	response := httptest.NewRecorder()
	s.mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/adapters/unknown/disable", strings.NewReader("")))
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown adapter status=%d body=%s", response.Code, response.Body.String())
	}
}
