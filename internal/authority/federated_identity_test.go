package authority

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

	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/integrated-recorder/core/internal/discovery"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func TestFederatedResolverRequiresAllProviderHistoriesOnEveryAuthorization(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	bundle := authorityFederatedHistoryFixture(t, 0x51, 0x61, now)
	first := newAuthorityHistoryDirectory(t, bundle)
	second := newAuthorityHistoryDirectory(t, bundle)
	transport := &authorityDirectoryTransport{handlers: map[string]http.Handler{"one.example": discovery.HTTPHandler(first), "two.example": discovery.HTTPHandler(second)}, failures: map[string]bool{}}
	federation, err := discovery.NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := globalidentity.OpenHistoryRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resolver := NewFederatedIdentityResolver(registry, federation)
	state, err := resolver.Resolve(bundle.IdentityID, "", now)
	if err != nil || state.IdentityID != bundle.IdentityID || state.Frozen || len(state.CurrentKeys) != 1 {
		t.Fatalf("agreed identity did not resolve: state=%+v err=%v", state, err)
	}
	if state.VerifiedAt != now {
		t.Fatalf("freshness timestamp is not the local observation time: got=%s want=%s", state.VerifiedAt, now)
	}
	transport.failures["two.example"] = true
	if _, err := resolver.Resolve(bundle.IdentityID, "", now.Add(time.Second)); !errors.Is(err, ErrStaleIdentity) {
		t.Fatalf("cached provider agreement authorized while a provider was unavailable: %v", err)
	}
}

func TestFederatedResolverImportsDivergenceThenFailsClosedOnFrozenIdentity(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	firstBundle := authorityFederatedHistoryFixture(t, 0x52, 0x62, now)
	secondBundle := authorityFederatedHistoryFixture(t, 0x52, 0x63, now)
	first := newAuthorityHistoryDirectory(t, firstBundle)
	second := newAuthorityHistoryDirectory(t, secondBundle)
	transport := &authorityDirectoryTransport{handlers: map[string]http.Handler{"one.example": discovery.HTTPHandler(first), "two.example": discovery.HTTPHandler(second)}}
	federation, err := discovery.NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := globalidentity.OpenHistoryRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resolver := NewFederatedIdentityResolver(registry, federation)
	if _, err := resolver.Resolve(firstBundle.IdentityID, "", now); !errors.Is(err, ErrStaleIdentity) && !errors.Is(err, ErrFrozenIdentity) {
		t.Fatalf("provider disagreement authorized a remote identity: %v", err)
	}
	projection, err := registry.AuthorityProjection(firstBundle.IdentityID)
	if err != nil || !projection.Frozen {
		t.Fatalf("valid competing histories were not retained as frozen evidence: projection=%+v err=%v", projection, err)
	}
	if _, err := resolver.Resolve(firstBundle.IdentityID, "", now.Add(time.Second)); !errors.Is(err, ErrFrozenIdentity) && !errors.Is(err, ErrStaleIdentity) {
		t.Fatalf("frozen identity resolved unexpectedly: %v", err)
	}
}

func TestFederatedResolverDoesNotTreatDurableImportAsFreshAfterRestart(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	bundle := authorityFederatedHistoryFixture(t, 0x53, 0x64, now)
	registry, err := globalidentity.OpenHistoryRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Import(bundle); err != nil {
		t.Fatal(err)
	}
	first := newAuthorityHistoryDirectory(t, bundle)
	second := newAuthorityHistoryDirectory(t, bundle)
	transport := &authorityDirectoryTransport{handlers: map[string]http.Handler{"one.example": discovery.HTTPHandler(first), "two.example": discovery.HTTPHandler(second)}, failures: map[string]bool{"one.example": true, "two.example": true}}
	federation, err := discovery.NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	// Durable imported history is evidence, not proof that all configured
	// providers still agree at the time of this authorization decision.
	resolver := NewFederatedIdentityResolver(registry, federation)
	if _, err := resolver.Resolve(bundle.IdentityID, "", now); !errors.Is(err, ErrStaleIdentity) {
		t.Fatalf("durable imported history became fresh after restart without federation lookup: %v", err)
	}
}

func newAuthorityHistoryDirectory(t *testing.T, bundle globalidentity.HistoryBundle) *discovery.Directory {
	t.Helper()
	directory, err := discovery.OpenDirectory(filepath.Join(t.TempDir(), "directory"))
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.PublishIdentityHistory(bundle); err != nil {
		t.Fatal(err)
	}
	return directory
}

func authorityFederatedHistoryFixture(t *testing.T, rootSeed, challengeSeed byte, now time.Time) globalidentity.HistoryBundle {
	t.Helper()
	rootPrivate := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{rootSeed}, ed25519.SeedSize))
	rootPublic := rootPrivate.Public().(ed25519.PublicKey)
	identityID, err := identityprotocol.IdentityID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	recoveryPrivate, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	credentialID := []byte{challengeSeed, 0x66}
	challenge := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{challengeSeed}, 32))
	genesis := signAuthorityHistoryEvent(t, rootPrivate, identityID, "0", nil, "genesis", map[string]any{
		"identity_format":                "global-identity/v1",
		"recovery_encryption_public_key": base64.RawURLEncoding.EncodeToString(recoveryPrivate.PublicKey().Bytes()),
		"webauthn_challenge":             challenge,
		"credential_id":                  base64.RawURLEncoding.EncodeToString(credentialID),
	}, now)
	store, err := globalidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	credential := webauthn.Credential{ID: credentialID, PublicKey: []byte("authority fixture credential")}
	if err := store.CreateGenesis(genesis.Canonical, genesis.Signature, bytes.Repeat([]byte{0x77}, 32), credential, challenge); err != nil {
		t.Fatal(err)
	}
	bundle, err := store.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

type authorityDirectoryTransport struct {
	handlers map[string]http.Handler
	failures map[string]bool
}

func (transport *authorityDirectoryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	host := request.URL.Hostname()
	if transport.failures[host] {
		return nil, errors.New("test discovery endpoint unavailable")
	}
	handler := transport.handlers[host]
	if handler == nil {
		return nil, errors.New("test discovery endpoint missing")
	}
	copy := request.Clone(request.Context())
	copy.RemoteAddr = "192.0.2.52:443"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, copy)
	response := recorder.Result()
	response.Request = request
	return response, nil
}

var _ = json.Marshal
var _ = strings.TrimSpace
