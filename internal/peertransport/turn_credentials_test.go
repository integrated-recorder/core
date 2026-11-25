package peertransport

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func TestTURNAuthConfigDerivesShortLivedNodeScopedCoturnCredential(t *testing.T) {
	publicKey := make(ed25519.PublicKey, ed25519.PublicKeySize)
	nodeID, err := identityprotocol.NodeID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte("0123456789abcdef0123456789abcdef")
	now := time.Date(2026, time.November, 4, 12, 30, 0, 0, time.UTC)
	config, err := NewTURNAuthConfig(
		[]string{"turns:turn.example.test:5349?transport=tcp"},
		[]string{"stun:stun.example.test:3478"}, secret, 0, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	ice, err := config.ICE(nodeID, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(ice.Servers) != 2 || ice.Servers[0].Username != "" || ice.Servers[0].Credential != "" {
		t.Fatalf("STUN configuration unexpectedly contains credentials: %+v", ice.Servers)
	}
	turn := ice.Servers[1]
	wantExpiry := now.Add(DefaultTURNCredentialTTL).Unix()
	wantUsername := strconv.FormatInt(wantExpiry, 10) + ":" + nodeID
	if turn.Username != wantUsername || turn.URLs[0] != "turns:turn.example.test:5349?transport=tcp" {
		t.Fatalf("TURN username/URL = %q %v, want %q", turn.Username, turn.URLs, wantUsername)
	}
	mac := hmac.New(sha1.New, secret)
	_, _ = mac.Write([]byte(wantUsername))
	wantPassword := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if turn.Credential != wantPassword {
		t.Fatal("derived TURN password differs from coturn REST HMAC")
	}
	otherPublicKey := make(ed25519.PublicKey, ed25519.PublicKeySize)
	otherPublicKey[0] = 1
	otherNode, err := identityprotocol.NodeID(otherPublicKey)
	if err != nil {
		t.Fatal(err)
	}
	other, err := config.ICE(otherNode, now)
	if err != nil {
		t.Fatal(err)
	}
	if other.Servers[1].Username == turn.Username || other.Servers[1].Credential == turn.Credential {
		t.Fatal("different Node received the same TURN credential")
	}
	if _, err := config.ICE(nodeID, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%+v %#v", config, config); got == "" || strings.Contains(got, string(secret)) {
		t.Fatal("TURN configuration string exposed the shared secret")
	}
}

func TestTURNAuthConfigAllowsSTUNWithoutCredentials(t *testing.T) {
	publicKey := make(ed25519.PublicKey, ed25519.PublicKeySize)
	nodeID, _ := identityprotocol.NodeID(publicKey)
	config, err := NewTURNAuthConfig(nil, []string{"stun:stun.example.test:3478"}, nil, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	ice, err := config.ICE(nodeID, time.Now())
	if err != nil || len(ice.Servers) != 1 || ice.Servers[0].Username != "" {
		t.Fatalf("STUN-only ICE config has an invalid shape, err=%v", err)
	}
}

func TestTURNAuthConfigRejectsUnsafeOrInconsistentConfiguration(t *testing.T) {
	validSecret := []byte("0123456789abcdef0123456789abcdef")
	cases := []struct {
		name      string
		turn      []string
		stun      []string
		secret    []byte
		ttl       time.Duration
		relayOnly bool
	}{
		{name: "turn missing shared secret", turn: []string{"turn:turn.example.test:3478"}},
		{name: "short shared secret", turn: []string{"turn:turn.example.test:3478"}, secret: []byte("too-short")},
		{name: "secret without turn", secret: validSecret},
		{name: "relay only without turn", relayOnly: true},
		{name: "invalid TURN URL", turn: []string{"https://turn.example.test"}, secret: validSecret},
		{name: "URL embeds credentials", turn: []string{"turn:user:pass@turn.example.test:3478"}, secret: validSecret},
		{name: "invalid STUN query", stun: []string{"stun:stun.example.test:3478?transport=udp"}},
		{name: "too many URLs", turn: []string{"turn:a.test", "turn:b.test", "turn:c.test", "turn:d.test", "turn:e.test", "turn:f.test", "turn:g.test", "turn:h.test", "turn:i.test"}, secret: validSecret},
		{name: "credential TTL too long", turn: []string{"turn:turn.example.test:3478"}, secret: validSecret, ttl: 11 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewTURNAuthConfig(tc.turn, tc.stun, tc.secret, tc.ttl, tc.relayOnly); err == nil {
				t.Fatal("invalid TURN configuration was accepted")
			}
		})
	}
}

func TestTURNAuthConfigRejectsInvalidNodeAndTime(t *testing.T) {
	config, err := NewTURNAuthConfig([]string{"turn:turn.example.test:3478"}, nil, []byte("0123456789abcdef0123456789abcdef"), 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := config.ICE("nd_invalid", time.Now()); err == nil {
		t.Fatal("invalid Node ID was accepted")
	}
	publicKey := make(ed25519.PublicKey, ed25519.PublicKeySize)
	nodeID, _ := identityprotocol.NodeID(publicKey)
	if _, err := config.ICE(nodeID, time.Time{}); err == nil {
		t.Fatal("zero credential timestamp was accepted")
	}
}
