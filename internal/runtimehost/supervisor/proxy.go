package supervisor

import (
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type fixedProxy struct {
	target    url.URL
	transport *http.Transport
}

func makeProxy(target *url.URL) *fixedProxy {
	copyTarget := *target
	return &fixedProxy{
		target: copyTarget,
		transport: &http.Transport{
			Proxy:                 nil,
			DialContext:           (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			ForceAttemptHTTP2:     false,
			MaxIdleConns:          16,
			MaxIdleConnsPerHost:   8,
			IdleConnTimeout:       60 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second,
			DisableCompression:    true,
		},
	}
}

func (p *fixedProxy) ServeHTTP(w http.ResponseWriter, incoming *http.Request) {
	request := incoming.Clone(incoming.Context())
	target := p.target
	target.Path = incoming.URL.Path
	target.RawPath = incoming.URL.RawPath
	target.RawQuery = incoming.URL.RawQuery
	target.Fragment = ""
	request.URL = &target
	request.RequestURI = ""
	// Preserve the browser authority while still dialing the fixed trusted
	// Control Plane target. Security-sensitive same-origin checks (notably
	// first Owner bootstrap) must compare the browser Origin against the Host
	// actually used to reach Runtime Host. Untrusted Forwarded/X-Forwarded
	// headers are stripped below and are not involved in this decision.
	request.Host = incoming.Host
	request.Header = incoming.Header.Clone()
	removeHopHeaders(request.Header)
	// This private marker is written only after discarding any client-supplied
	// value. The Control Plane is behind this fixed Runtime Host proxy and uses
	// it solely to apply Secure cookie attributes for browser requests that
	// arrived over the trusted Runtime Host TLS listener.
	request.Header.Del("X-IR-HTTPS")
	if incoming.TLS != nil {
		request.Header.Set("X-IR-HTTPS", "1")
	}
	request.Header.Del("Forwarded")
	request.Header.Del("X-Forwarded-For")
	request.Header.Del("X-Forwarded-Host")
	request.Header.Del("X-Forwarded-Proto")

	response, err := p.transport.RoundTrip(request)
	if err != nil {
		http.Error(w, "control plane unavailable", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	for name, values := range response.Header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	removeHopHeaders(w.Header())
	w.WriteHeader(response.StatusCode)
	copyResponse(w, response.Body)
	for name, values := range response.Trailer {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
}

func copyResponse(w http.ResponseWriter, body io.Reader) {
	flusher, canFlush := w.(http.Flusher)
	buffer := make([]byte, 32<<10)
	for {
		n, readErr := body.Read(buffer)
		if n > 0 {
			if _, writeErr := w.Write(buffer[:n]); writeErr != nil {
				return
			}
			if canFlush {
				flusher.Flush()
			}
		}
		if readErr != nil {
			return
		}
	}
}

func removeHopHeaders(header http.Header) {
	for _, connection := range header.Values("Connection") {
		for _, token := range strings.Split(connection, ",") {
			header.Del(strings.TrimSpace(token))
		}
	}
	for _, name := range []string{
		"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
		"Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade",
	} {
		header.Del(name)
	}
}
