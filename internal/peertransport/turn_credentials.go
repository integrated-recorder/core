package peertransport

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/pion/webrtc/v4"
)

const (
	DefaultTURNCredentialTTL = 5 * time.Minute
	maxTURNCredentialTTL     = 10 * time.Minute
	maxTURNSharedSecretBytes = 1024
	maxICEURLBytes           = 2048
)

// TURNAuthConfig contains server-side coturn REST credentials. SharedSecret
// must remain local to the Control Plane; ICE() derives a short-lived
// per-Node credential that is passed only to Pion's ICE agent.
type TURNAuthConfig struct {
	turnURLs      []string
	stunURLs      []string
	sharedSecret  []byte
	credentialTTL time.Duration
	relayOnly     bool
}

// NewTURNAuthConfig validates and copies the Control Plane's long-lived
// coturn shared secret. The secret is deliberately unexported so generic
// struct formatting cannot reveal it.
func NewTURNAuthConfig(turnURLs, stunURLs []string, sharedSecret []byte, ttl time.Duration, relayOnly bool) (*TURNAuthConfig, error) {
	config := &TURNAuthConfig{
		turnURLs: append([]string(nil), turnURLs...), stunURLs: append([]string(nil), stunURLs...),
		sharedSecret: append([]byte(nil), sharedSecret...), credentialTTL: ttl, relayOnly: relayOnly,
	}
	if err := config.validate(); err != nil {
		return nil, ErrConfig
	}
	return config, nil
}

// String and GoString intentionally omit URLs and all authentication material.
func (c TURNAuthConfig) String() string {
	return fmt.Sprintf("TURNAuthConfig{turn_urls:%d stun_urls:%d relay_only:%t credential_ttl:%s secret_configured:%t}",
		len(c.turnURLs), len(c.stunURLs), c.relayOnly, c.credentialTTL, len(c.sharedSecret) > 0)
}

func (c TURNAuthConfig) GoString() string { return c.String() }

// ICE derives a coturn REST-API TURN credential for one Node and
// validates the resulting ICE configuration. It never persists or logs the
// shared secret or derived credential.
func (c TURNAuthConfig) ICE(nodeID string, now time.Time) (ICEConfig, error) {
	if now.IsZero() || !identityprotocol.IsNodeID(nodeID) || c.validate() != nil {
		return ICEConfig{}, ErrConfig
	}
	config := ICEConfig{RelayOnly: c.relayOnly}
	if len(c.stunURLs) != 0 {
		config.Servers = append(config.Servers, webrtcICEServer(c.stunURLs, "", ""))
	}
	if len(c.turnURLs) != 0 {
		ttl := c.credentialTTL
		if ttl == 0 {
			ttl = DefaultTURNCredentialTTL
		}
		if ttl < time.Minute || ttl > maxTURNCredentialTTL {
			return ICEConfig{}, ErrConfig
		}
		expires := now.Add(ttl).Unix()
		if expires <= now.Unix() {
			return ICEConfig{}, ErrConfig
		}
		username := strconv.FormatInt(expires, 10) + ":" + nodeID
		mac := hmac.New(sha1.New, c.sharedSecret)
		_, _ = mac.Write([]byte(username))
		password := base64.StdEncoding.EncodeToString(mac.Sum(nil))
		config.Servers = append(config.Servers, webrtcICEServer(c.turnURLs, username, password))
	}
	if validateICEConfig(config) != nil {
		return ICEConfig{}, ErrConfig
	}
	return config, nil
}

// validate checks the long-lived server-side configuration without deriving
// or exposing a credential. It is safe to call during optional peer-service
// startup before a particular Node session exists.
func (c TURNAuthConfig) validate() error {
	if len(c.turnURLs) > 8 || len(c.stunURLs) > 8 || len(c.sharedSecret) > maxTURNSharedSecretBytes {
		return ErrConfig
	}
	if len(c.stunURLs) > 0 && !validICEURLs(c.stunURLs, "stun", "stuns") {
		return ErrConfig
	}
	if len(c.turnURLs) == 0 {
		if len(c.sharedSecret) != 0 || c.relayOnly || c.credentialTTL != 0 {
			return ErrConfig
		}
	} else {
		if len(c.sharedSecret) < 32 || !validICEURLs(c.turnURLs, "turn", "turns") {
			return ErrConfig
		}
		if c.credentialTTL != 0 && (c.credentialTTL < time.Minute || c.credentialTTL > maxTURNCredentialTTL) {
			return ErrConfig
		}
	}
	if c.relayOnly && len(c.turnURLs) == 0 {
		return ErrConfig
	}
	return nil
}

func validICEURLs(values []string, schemes ...string) bool {
	allowed := make(map[string]bool, len(schemes))
	for _, scheme := range schemes {
		allowed[scheme] = true
	}
	for _, raw := range values {
		if len(raw) == 0 || len(raw) > maxICEURLBytes {
			return false
		}
		parsed, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || parsed.User != nil || parsed.Fragment != "" || !allowed[strings.ToLower(parsed.Scheme)] {
			return false
		}
		authority := parsed
		if parsed.Opaque != "" {
			authority, err = url.Parse("//" + parsed.Opaque)
			if err != nil || authority.User != nil || authority.Path != "" || authority.RawQuery != "" {
				return false
			}
		}
		if authority.Hostname() == "" || (authority.Path != "" && authority.Path != "/") {
			return false
		}
		if port := authority.Port(); port != "" {
			parsedPort, err := strconv.Atoi(port)
			if err != nil || parsedPort < 1 || parsedPort > 65535 {
				return false
			}
		} else if strings.HasSuffix(authority.Host, ":") {
			return false
		}
		if parsed.RawQuery != "" {
			query, err := url.ParseQuery(parsed.RawQuery)
			if err != nil || strings.ToLower(parsed.Scheme) != "turn" && strings.ToLower(parsed.Scheme) != "turns" ||
				len(query) != 1 || len(query["transport"]) != 1 || (query.Get("transport") != "udp" && query.Get("transport") != "tcp") {
				return false
			}
		}
	}
	return len(values) > 0
}

func webrtcICEServer(urls []string, username, credential string) webrtc.ICEServer {
	return webrtc.ICEServer{URLs: append([]string(nil), urls...), Username: username, Credential: credential}
}
