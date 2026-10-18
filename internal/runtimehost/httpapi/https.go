package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/runtimehost/tlsserver"
)

const HTTPSSettingsEndpoint = "/api/runtime/https"
const maxHTTPSRequest = 2 << 20

type HTTPSSettings struct {
	Enabled          bool     `json:"enabled"`
	Listening        bool     `json:"listening"`
	CertificateReady bool     `json:"certificate_ready"`
	IssuanceFailed   bool     `json:"issuance_failed"`
	RedirectHTTP     bool     `json:"redirect_http"`
	Port             int      `json:"port"`
	Hosts            []string `json:"hosts"`
	SelfSigned       bool     `json:"self_signed"`
	ACME             bool     `json:"acme"`
	ExpiresAt        string   `json:"expires_at,omitempty"`
}

type HTTPSController interface {
	Status() tlsserver.Status
	EnableSelfSigned([]string, bool) error
	EnableACME(string, bool) error
	EnableCertificate([]string, string, string, bool) error
	Disable() error
}

type httpsRequest struct {
	Hosts          []string `json:"hosts"`
	RedirectHTTP   bool     `json:"redirect_http"`
	ACME           bool     `json:"acme,omitempty"`
	CertificatePEM string   `json:"certificate_pem,omitempty"`
	PrivateKeyPEM  string   `json:"private_key_pem,omitempty"`
}

func WithHTTPS(auth *authn.Service, authDisabled bool, controller HTTPSController, next http.Handler) (http.Handler, error) {
	if controller == nil || next == nil || (!authDisabled && auth == nil) {
		return nil, errors.New("HTTPS API dependencies are incomplete")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != HTTPSSettingsEndpoint {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		if r.Method == http.MethodGet {
			if !authorizeHTTPS(w, r, auth, authDisabled, false) {
				return
			}
			writeSetupJSON(w, http.StatusOK, controller.Status())
			return
		}
		if r.Method == http.MethodDelete {
			if !authorizeHTTPS(w, r, auth, authDisabled, true) {
				return
			}
			if !emptyHTTPSBody(w, r) {
				return
			}
			if err := controller.Disable(); err != nil {
				writeAPIError(w, http.StatusServiceUnavailable, "https_disable_failed", "HTTPS could not be disabled safely.")
				return
			}
			writeSetupJSON(w, http.StatusOK, controller.Status())
			return
		}
		if r.Method != http.MethodPut {
			writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method not allowed.")
			return
		}
		if !authorizeHTTPS(w, r, auth, authDisabled, true) {
			return
		}
		if r.Body == nil || r.ContentLength > maxHTTPSRequest {
			writeAPIError(w, http.StatusRequestEntityTooLarge, "invalid_request", "HTTPS settings request is too large.")
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxHTTPSRequest))
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request", "HTTPS settings request is invalid.")
			return
		}
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		var req httpsRequest
		if err = dec.Decode(&req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request", "HTTPS settings request is invalid.")
			return
		}
		var trailing any
		if dec.Decode(&trailing) != io.EOF {
			writeAPIError(w, http.StatusBadRequest, "invalid_request", "HTTPS settings request is invalid.")
			return
		}
		if req.ACME {
			if len(req.Hosts) != 1 {
				err = errors.New("ACME requires one domain")
			} else {
				err = controller.EnableACME(req.Hosts[0], req.RedirectHTTP)
			}
		} else if req.CertificatePEM == "" && req.PrivateKeyPEM == "" {
			// Bind the certificate to the host the administrator actually used
			// to open the UI as well as any additional SANs they entered. The
			// request Host is validated by the same strict hostname/IP parser in
			// the TLS manager; it is never trusted as a proxy forwarding header.
			hosts := append([]string(nil), req.Hosts...)
			if host := httpsRequestHostName(r.Host); host != "" {
				hosts = append(hosts, host)
			}
			err = controller.EnableSelfSigned(hosts, req.RedirectHTTP)
		} else if req.CertificatePEM == "" || req.PrivateKeyPEM == "" {
			err = errors.New("incomplete certificate")
		} else if !secureHTTPSManagementRequest(r) {
			writeAPIError(w, http.StatusForbidden, "https_certificate_requires_secure_transport", "Upload private-key material from the local machine or an HTTPS connection.")
			return
		} else {
			err = controller.EnableCertificate(req.Hosts, req.CertificatePEM, req.PrivateKeyPEM, req.RedirectHTTP)
		}
		if err != nil {
			if req.ACME {
				writeAPIError(w, http.StatusBadRequest, "https_acme_preflight_failed", "The domain must resolve in public DNS and inbound TCP 443 must reach this Runtime Host before ACME can be enabled.")
				return
			}
			writeAPIError(w, http.StatusBadRequest, "https_configuration_rejected", "HTTPS configuration was rejected. Check the host names and certificate/key pair.")
			return
		}
		status := controller.Status()
		if !authDisabled && auth != nil && (r.TLS != nil || status.Ready) {
			if session, sessionErr := auth.Authenticate(authn.SessionToken(r)); sessionErr == nil {
				// Upgrade an existing HTTP session once a TLS listener has a valid
				// certificate. In particular, do not strand the administrator on
				// HTTP while ACME has not yet issued its first certificate.
				session.Token = authn.SessionToken(r)
				authn.SetSessionCookie(w, r, session, true)
				if status.Ready {
					// The CSRF cookie contains the browser's existing readable
					// token. Reissue it with Secure alongside the HttpOnly session
					// cookie so both cookies use TLS after a successful transition.
					if csrf, csrfErr := r.Cookie(authn.CSRFCookieName); csrfErr == nil && csrf.Value != "" {
						maxAge := int(time.Until(session.ExpiresAt).Seconds())
						if maxAge < 1 {
							maxAge = 1
						}
						http.SetCookie(w, &http.Cookie{Name: authn.CSRFCookieName, Value: csrf.Value, Path: "/", Expires: session.ExpiresAt, MaxAge: maxAge, Secure: true, SameSite: http.SameSiteStrictMode})
					}
				}
			}
		}
		writeSetupJSON(w, http.StatusOK, controller.Status())
	}), nil
}

func secureHTTPSManagementRequest(r *http.Request) bool {
	// Enable a generated self-signed certificate first, then upload replacement
	// key material over that TLS listener. Never send private keys over cleartext
	// HTTP, including loopback, where Docker port forwarding can obscure the
	// browser's original address. The Host proxy strips any client-supplied
	// marker and sets it only for an actual TLS connection.
	return r.TLS != nil || r.Header.Get("X-IR-HTTPS") == "1"
}

func httpsRequestHostName(authority string) string {
	if host, _, err := net.SplitHostPort(authority); err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.Trim(authority, "[]")
}

func authorizeHTTPS(w http.ResponseWriter, r *http.Request, auth *authn.Service, disabled, mutation bool) bool {
	if disabled {
		writeAPIError(w, http.StatusForbidden, "https_unavailable", "HTTPS management requires authentication.")
		return false
	}
	token := authn.SessionToken(r)
	session, err := auth.Authenticate(token)
	if err != nil {
		writeAPIError(w, http.StatusUnauthorized, "authentication_required", "Authentication is required.")
		return false
	}
	principal := authn.Principal{UserID: session.UserID, Login: session.Login, Roles: session.Roles, Permissions: session.Permissions, Role: session.Role}
	if !authn.HasPermission(principal, authn.PermissionSystemSettings) {
		writeAPIError(w, http.StatusForbidden, "permission_denied", "Permission denied.")
		return false
	}
	if mutation && !auth.ValidCSRF(token, r.Header.Get("X-CSRF-Token")) {
		writeAPIError(w, http.StatusForbidden, "csrf_rejected", "Request validation failed.")
		return false
	}
	return true
}

func emptyHTTPSBody(w http.ResponseWriter, r *http.Request) bool {
	if r.Body == nil {
		return true
	}
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
	if err != nil || len(bytes.TrimSpace(b)) != 0 {
		writeAPIError(w, http.StatusBadRequest, "invalid_request", "Request body must be empty.")
		return false
	}
	return true
}
