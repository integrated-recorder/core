package discovery

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/signalingprotocol"
)

func makeSignalFixture(t *testing.T, now time.Time) (signalingprotocol.Message, ed25519.PrivateKey) {
	t.Helper()
	rootPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityID, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	sourcePublic, sourcePrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	targetPublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sourceNode, _ := identityprotocol.NodeID(sourcePublic)
	targetNode, _ := identityprotocol.NodeID(targetPublic)
	ephemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	session, err := signalingprotocol.NewSessionID(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	message, err := signalingprotocol.NewMessage(signalingprotocol.KindInit, session, identityID, "sha256:"+strings.Repeat("a", 64), sourceNode, identityID, targetNode, now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	message.EphemeralPublicKey = base64.RawURLEncoding.EncodeToString(ephemeral.PublicKey().Bytes())
	message, err = signalingprotocol.Sign(message, sourcePrivate)
	if err != nil {
		t.Fatal(err)
	}
	return message, sourcePrivate
}

func TestSignalRelayIsBoundedIdempotentAndEphemeral(t *testing.T) {
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	directory, err := OpenDirectory(filepath.Join(t.TempDir(), "directory"))
	if err != nil {
		t.Fatal(err)
	}
	message, privateKey := makeSignalFixture(t, now)
	inserted, err := directory.PublishSignal(message, now)
	if err != nil || inserted {
		t.Fatalf("first publish inserted=%v err=%v", inserted, err)
	}
	duplicate, err := directory.PublishSignal(message, now)
	if err != nil || !duplicate {
		t.Fatalf("idempotent publish=%v err=%v", duplicate, err)
	}
	messages, err := directory.LookupSignals(message.TargetNodeID, now)
	if err != nil || len(messages) != 1 || messages[0].MessageID != message.MessageID {
		t.Fatalf("lookup=%+v err=%v", messages, err)
	}
	if messages, err := directory.LookupSignals(message.SourceNodeID, now); err != nil || len(messages) != 0 {
		t.Fatalf("lookup leaked to wrong recipient: %+v err=%v", messages, err)
	}

	conflict := message
	conflict.Signature = ""
	ephemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	conflict.EphemeralPublicKey = base64.RawURLEncoding.EncodeToString(ephemeral.PublicKey().Bytes())
	conflict, err = signalingprotocol.Sign(conflict, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := directory.PublishSignal(conflict, now); !errors.Is(err, ErrSignalConflict) {
		t.Fatalf("same message ID with different bytes accepted: %v", err)
	}
	if _, err := directory.PublishSignal(message, now.Add(signalingprotocol.MaxMessageTTL+time.Second)); !errors.Is(err, signalingprotocol.ErrStale) {
		t.Fatalf("expired signal accepted: %v", err)
	}
	if messages, err := directory.LookupSignals(message.TargetNodeID, now.Add(signalingprotocol.MaxMessageTTL+time.Second)); err != nil || len(messages) != 0 {
		t.Fatalf("expired signals retained: %+v err=%v", messages, err)
	}
}

func TestSignalHTTPRelayRequiresCanonicalBoundedEnvelope(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	directory, err := OpenDirectory(filepath.Join(t.TempDir(), "directory"))
	if err != nil {
		t.Fatal(err)
	}
	handler := HTTPHandler(directory)
	message, _ := makeSignalFixture(t, now)
	body, err := signalingprotocol.CanonicalMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/signals", bytes.NewReader(body))
	request.RemoteAddr = "192.0.2.10:1234"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("publish status=%d body=%q", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/signals?target_node_id="+message.TargetNodeID, nil)
	request.RemoteAddr = "192.0.2.10:1234"
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("lookup status=%d headers=%v body=%q", response.Code, response.Header(), response.Body.String())
	}
	var result signalLookupResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || len(result.Messages) != 1 || result.Messages[0].MessageID != message.MessageID {
		t.Fatalf("lookup=%+v err=%v", result, err)
	}
	request = httptest.NewRequest(http.MethodPost, "/v1/signals", bytes.NewBufferString(string(body)+" "))
	request.RemoteAddr = "192.0.2.11:1234"
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("noncanonical publish status=%d body=%q", response.Code, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/signals?target_node_id=bad", nil)
	request.RemoteAddr = "192.0.2.12:1234"
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid target query status=%d", response.Code)
	}
}
