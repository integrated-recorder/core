package acquire

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestDoMediaRequestDefaultsToSameOriginHeaders(t *testing.T) {
	const headerName = "X-Adapter-Secret"
	const headerValue = "test-value"
	seen := make(chan string, 2)
	server := newRequestPolicyTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get(headerName)
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()
	other := newRequestPolicyTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get(headerName)
		_, _ = io.WriteString(w, "ok")
	}))
	defer other.Close()

	for _, test := range []struct {
		name string
		url  string
		want string
	}{
		{name: "same origin", url: server.URL + "/same", want: headerValue},
		{name: "cross origin denied", url: other.URL + "/cross"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodGet, test.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := doMediaRequest(&http.Client{}, request, map[string]string{headerName: headerValue}, server.URL+"/manifest.m3u8", nil)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if got := <-seen; got != test.want {
				t.Fatalf("forwarded header = %q, want %q", got, test.want)
			}
		})
	}
}

func TestDoMediaRequestReevaluatesAllowlistAtEveryRedirect(t *testing.T) {
	const headerName = "Authorization"
	const headerValue = "Bearer test-value"
	type observed struct {
		server string
		value  string
	}
	seen := make(chan observed, 4)
	var secondURL, thirdURL string
	first := newRequestPolicyTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- observed{"first", r.Header.Get(headerName)}
		http.Redirect(w, r, secondURL, http.StatusFound)
	}))
	defer first.Close()
	second := newRequestPolicyTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- observed{"second", r.Header.Get(headerName)}
		http.Redirect(w, r, thirdURL, http.StatusFound)
	}))
	defer second.Close()
	third := newRequestPolicyTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- observed{"third", r.Header.Get(headerName)}
		_, _ = io.WriteString(w, "ok")
	}))
	defer third.Close()
	secondURL = second.URL + "/hop"
	thirdURL = third.URL + "/final"

	media := adapterproto.MediaSource{ManifestURL: first.URL + "/manifest.m3u8", RequestPolicy: &adapterproto.RequestPolicy{HeaderForwarding: &adapterproto.HeaderForwardingPolicy{Mode: adapterproto.HeaderForwardingAllowlist, Origins: []string{first.URL, second.URL}}}}
	if err := adapterproto.ValidateMediaSource(adapterproto.MediaSource{Type: "hls", ManifestURL: media.ManifestURL, Headers: map[string]string{headerName: headerValue}, RequestPolicy: media.RequestPolicy}, []string{"hls"}); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, media.ManifestURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := doMediaRequest(&http.Client{}, request, map[string]string{headerName: headerValue}, media.ManifestURL, media.RequestPolicy)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()

	for _, want := range []observed{{"first", headerValue}, {"second", headerValue}, {"third", ""}} {
		if got := <-seen; got != want {
			t.Fatalf("redirect header observation = %#v, want %#v", got, want)
		}
	}

	// A request starting outside the allowlist receives no adapter header even
	// though it uses the same resolved media source.
	request, err = http.NewRequest(http.MethodGet, third.URL+"/direct", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = doMediaRequest(&http.Client{}, request, map[string]string{headerName: headerValue}, media.ManifestURL, media.RequestPolicy)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if got := <-seen; got != (observed{"third", ""}) {
		t.Fatalf("unlisted direct request header = %#v", got)
	}
}

type fixedMediaResolver struct{ media adapterproto.MediaSource }

func newRequestPolicyTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.Start()
	return server
}

func (r fixedMediaResolver) Resolve(context.Context, string, json.RawMessage, *adapterproto.ResourceRef) (adapterproto.MediaSource, error) {
	return r.media, nil
}

func TestRequestPolicyDoesNotBypassCoreSSRFValidation(t *testing.T) {
	const localManifest = "http://127.0.0.1:12345/live.m3u8"
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(store, nil, fixedMediaResolver{media: adapterproto.MediaSource{
		Type: "hls", ManifestURL: localManifest,
		RequestPolicy: &adapterproto.RequestPolicy{HeaderForwarding: &adapterproto.HeaderForwardingPolicy{Mode: adapterproto.HeaderForwardingAllowlist, Origins: []string{"http://127.0.0.1:12345"}}},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.Start(context.Background(), "fixture", json.RawMessage(`{"source":"opaque"}`), nil, "private source")
	if err == nil || strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("loopback manifest was not safely rejected by Core validation: %v", err)
	}
}
