package discovery

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-webauthn/webauthn/webauthn"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/signalingprotocol"
)

func TestFederationRequiresTwoDistinctHTTPSProviders(t *testing.T) {
	for _, urls := range [][]string{
		{"https://one.example"},
		{"https://one.example", "https://one.example:8443"},
		{"http://one.example", "https://two.example"},
		{"https://user:secret@one.example", "https://two.example"},
	} {
		if _, err := NewFederationClient(urls, nil); !errors.Is(err, ErrFederationConfig) {
			t.Fatalf("accepted invalid federation URLs %v: %v", urls, err)
		}
	}
	if _, err := NewFederationClient([]string{"https://one.example", "https://two.example"}, nil); err != nil {
		t.Fatalf("rejected two distinct HTTPS providers: %v", err)
	}
}

func TestFederationPublishesToTwoProvidersAndMergesLatestSignedAdvertisement(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	privateKey := directoryTestKey(0x71)
	firstCanonical, firstSignature := signDirectoryAdvertisement(t, privateKey, "1", now, 0x81)
	secondCanonical, secondSignature := signDirectoryAdvertisement(t, privateKey, "2", now.Add(time.Minute), 0x82)
	firstDirectory, secondDirectory := newTestDirectory(t), newTestDirectory(t)
	transport := &directoryTransport{handlers: map[string]http.Handler{"one.example": HTTPHandler(firstDirectory), "two.example": HTTPHandler(secondDirectory)}}
	client, err := NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	statuses, err := client.Publish(context.Background(), firstCanonical, firstSignature, now)
	if err != nil || len(statuses) != 2 || !statuses[0].Success || !statuses[1].Success {
		t.Fatalf("federated publish statuses=%+v err=%v", statuses, err)
	}
	if _, err := secondDirectory.Publish(secondCanonical, secondSignature, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	lookup, err := client.Lookup(context.Background(), directoryTestIdentityHint(t), now.Add(2*time.Minute))
	if err != nil || !lookup.Complete || len(lookup.Advertisements) != 1 || lookup.Advertisements[0].Advertisement.Sequence != "2" || len(lookup.ForkedNodeIDs) != 0 {
		t.Fatalf("federated lookup=%+v err=%v", lookup, err)
	}
}

func TestFederationUsesVerifiedHTTPSAcrossTwoLoopbackProviders(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	canonical, signature := signDirectoryAdvertisement(t, directoryTestKey(0x75), "1", now, 0x87)
	roots := x509.NewCertPool()
	first, firstTarget := newFederationTLSServer(t, "one.example", newTestDirectory(t), roots)
	second, secondTarget := newFederationTLSServer(t, "two.example", newTestDirectory(t), roots)
	defer first.Close()
	defer second.Close()
	targets := map[string]string{"one.example": firstTarget, "two.example": secondTarget}
	dialer := &net.Dialer{Timeout: time.Second}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(address)
			if err != nil || targets[host] == "" {
				return nil, errors.New("test provider address is unavailable")
			}
			return dialer.DialContext(ctx, network, targets[host])
		},
	}
	defer transport.CloseIdleConnections()
	client, err := NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport, Timeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	statuses, err := client.Publish(context.Background(), canonical, signature, now)
	if err != nil || len(statuses) != 2 || !statuses[0].Success || !statuses[1].Success {
		t.Fatalf("HTTPS federation publish statuses=%+v err=%v", statuses, err)
	}
	lookup, err := client.Lookup(context.Background(), directoryTestIdentityHint(t), now.Add(time.Second))
	if err != nil || !lookup.Complete || len(lookup.Advertisements) != 1 || lookup.Advertisements[0].Advertisement.NodeID == "" {
		t.Fatalf("HTTPS federation lookup=%+v err=%v", lookup, err)
	}
}

func TestFederationSurfacesPartialProviderFailureAndDoesNotPretendComplete(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	canonical, signature := signDirectoryAdvertisement(t, directoryTestKey(0x72), "1", now, 0x83)
	firstDirectory, secondDirectory := newTestDirectory(t), newTestDirectory(t)
	transport := &directoryTransport{handlers: map[string]http.Handler{"one.example": HTTPHandler(firstDirectory), "two.example": HTTPHandler(secondDirectory)}, failures: map[string]bool{"two.example": true}}
	client, err := NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	statuses, err := client.Publish(context.Background(), canonical, signature, now)
	if !errors.Is(err, ErrPublishIncomplete) || len(statuses) != 2 || statuses[0].Success && statuses[1].Success {
		t.Fatalf("partial publish statuses=%+v err=%v", statuses, err)
	}
	lookup, err := client.Lookup(context.Background(), directoryTestIdentityHint(t), now.Add(time.Second))
	if err != nil || lookup.Complete || len(lookup.Advertisements) != 1 || len(lookup.Providers) != 2 {
		t.Fatalf("partial lookup=%+v err=%v", lookup, err)
	}
	if !lookup.Providers[0].Success || lookup.Providers[1].Success || lookup.Providers[1].Code != "transport_failed" {
		t.Fatalf("partial provider status=%+v", lookup.Providers)
	}
}

func TestFederationPublishesAndMergesSignedPeerSignals(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	firstDirectory, secondDirectory := newTestDirectory(t), newTestDirectory(t)
	transport := &directoryTransport{handlers: map[string]http.Handler{"one.example": HTTPHandler(firstDirectory), "two.example": HTTPHandler(secondDirectory)}}
	client, err := NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := makeSignalFixture(t, now)
	statuses, err := client.PublishSignal(context.Background(), message, now)
	if err != nil || len(statuses) != 2 || !statuses[0].Success || !statuses[1].Success {
		t.Fatalf("federated signal publish statuses=%+v err=%v", statuses, err)
	}
	lookup, err := client.LookupSignals(context.Background(), message.TargetNodeID)
	if err != nil || !lookup.Complete || len(lookup.Messages) != 1 || lookup.Messages[0].MessageID != message.MessageID {
		t.Fatalf("federated signal lookup=%+v err=%v", lookup, err)
	}

	transport.failures = map[string]bool{"two.example": true}
	partial, err := client.LookupSignals(context.Background(), message.TargetNodeID)
	if err != nil || partial.Complete || len(partial.Messages) != 1 || len(partial.Providers) != 2 {
		t.Fatalf("partial signal lookup=%+v err=%v", partial, err)
	}
	if _, err := client.PublishSignal(context.Background(), message, now); !errors.Is(err, ErrSignalPublishIncomplete) {
		t.Fatalf("partial signal publish was reported successful: %v", err)
	}
}

func TestFederationRejectsConflictingPeerSignalCopies(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	message, sourceKey := makeSignalFixture(t, now)
	conflict, err := signalingprotocol.NewMessage(signalingprotocol.KindCancel, message.SessionID, message.SourceIdentityID, message.SourceHeadHash, message.SourceNodeID, message.TargetIdentityID, message.TargetNodeID, now, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	conflict.MessageID = message.MessageID
	conflict, err = signalingprotocol.Sign(conflict, sourceKey)
	if err != nil {
		t.Fatal(err)
	}
	firstDirectory, secondDirectory := newTestDirectory(t), newTestDirectory(t)
	if _, err := firstDirectory.PublishSignal(message, now); err != nil {
		t.Fatal(err)
	}
	if _, err := secondDirectory.PublishSignal(conflict, now); err != nil {
		t.Fatal(err)
	}
	transport := &directoryTransport{handlers: map[string]http.Handler{"one.example": HTTPHandler(firstDirectory), "two.example": HTTPHandler(secondDirectory)}}
	client, err := NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := client.LookupSignals(context.Background(), message.TargetNodeID)
	if !errors.Is(err, ErrSignalProvidersDisagree) || len(lookup.Messages) != 0 {
		t.Fatalf("conflicting signal copies returned as candidates: lookup=%+v err=%v", lookup, err)
	}
}

func TestFederatedIdentityHistoryRequiresExactAgreementAndReturnsValidDisagreement(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	firstBundle := signedIdentityHistoryBundle(t, 0x35, 0x41)
	secondBundle := signedIdentityHistoryBundle(t, 0x35, 0x42)
	if firstBundle.IdentityID != secondBundle.IdentityID {
		t.Fatal("fixture identities should share the same immutable identity ID")
	}
	firstDirectory, secondDirectory := newTestDirectory(t), newTestDirectory(t)
	if err := firstDirectory.PublishIdentityHistory(firstBundle); err != nil {
		t.Fatal(err)
	}
	if err := secondDirectory.PublishIdentityHistory(secondBundle); err != nil {
		t.Fatal(err)
	}
	transport := &directoryTransport{handlers: map[string]http.Handler{"one.example": HTTPHandler(firstDirectory), "two.example": HTTPHandler(secondDirectory)}}
	client, err := NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := client.LookupIdentityHistory(context.Background(), firstBundle.IdentityID, now)
	if !errors.Is(err, ErrHistoryDisagrees) || !lookup.Complete || lookup.Agreed || len(lookup.Bundles) != 2 {
		t.Fatalf("divergent signed histories were not surfaced: lookup=%+v err=%v", lookup, err)
	}
	registry, err := globalidentity.OpenHistoryRegistry(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	firstErr := registry.Import(lookup.Bundles[0])
	secondErr := registry.Import(lookup.Bundles[1])
	if firstErr != nil && !errors.Is(firstErr, globalidentity.ErrIdentityFork) || secondErr == nil && firstErr == nil {
		t.Fatalf("valid competing histories were not frozen: first=%v second=%v", firstErr, secondErr)
	}
	projection, err := registry.AuthorityProjection(firstBundle.IdentityID)
	if err != nil || !projection.Frozen {
		t.Fatalf("competing history projection=%+v err=%v", projection, err)
	}
}

func TestFederatedIdentityHistoryAgreementReturnsVerifiedPublicBundle(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	bundle := signedIdentityHistoryBundle(t, 0x36, 0x43)
	firstDirectory, secondDirectory := newTestDirectory(t), newTestDirectory(t)
	transport := &directoryTransport{handlers: map[string]http.Handler{"one.example": HTTPHandler(firstDirectory), "two.example": HTTPHandler(secondDirectory)}}
	client, err := NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	statuses, err := client.PublishIdentityHistory(context.Background(), bundle, now)
	if err != nil || len(statuses) != 2 || !statuses[0].Success || !statuses[1].Success {
		t.Fatalf("federated history publish statuses=%+v err=%v", statuses, err)
	}
	lookup, err := client.LookupIdentityHistory(context.Background(), bundle.IdentityID, now)
	if err != nil || !lookup.Complete || !lookup.Agreed || lookup.Summary.IdentityID != bundle.IdentityID || len(lookup.Bundles) != 2 {
		t.Fatalf("agreed history lookup=%+v err=%v", lookup, err)
	}
}

func signedIdentityHistoryBundle(t *testing.T, rootSeed, challengeSeed byte) globalidentity.HistoryBundle {
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
	credentialID := []byte{challengeSeed, 0x55}
	challenge := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{challengeSeed}, 32))
	keyID, err := identityprotocol.KeyID(rootPublic)
	if err != nil {
		t.Fatal(err)
	}
	issuedAt := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	event := map[string]any{
		"event_version": identityprotocol.EventVersion, "identity_id": identityID, "sequence": "0",
		"previous_event_hash": nil, "operation": "genesis", "issued_at": issuedAt.Format(time.RFC3339),
		"signer_key_id": keyID, "signer_public_key": base64.RawURLEncoding.EncodeToString(rootPublic),
		"payload": map[string]any{
			"identity_format":                "global-identity/v1",
			"recovery_encryption_public_key": base64.RawURLEncoding.EncodeToString(recoveryPrivate.PublicKey().Bytes()),
			"webauthn_challenge":             challenge,
			"credential_id":                  base64.RawURLEncoding.EncodeToString(credentialID),
		},
	}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	canonical, signature, err := identityprotocol.SignEvent(eventJSON, rootPrivate)
	if err != nil {
		t.Fatal(err)
	}
	store, err := globalidentity.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	credential := webauthn.Credential{ID: credentialID, PublicKey: []byte("fixture credential public key")}
	if err := store.CreateGenesis(canonical, signature, bytes.Repeat([]byte{0x77}, 32), credential, challenge); err != nil {
		t.Fatal(err)
	}
	bundle, err := store.ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func TestFederationFreezesSameSequenceConflictingSignedAdvertisements(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	privateKey := directoryTestKey(0x73)
	firstCanonical, firstSignature := signDirectoryAdvertisement(t, privateKey, "8", now, 0x84)
	secondCanonical, secondSignature := signDirectoryAdvertisement(t, privateKey, "8", now, 0x85)
	firstDirectory, secondDirectory := newTestDirectory(t), newTestDirectory(t)
	if _, err := firstDirectory.Publish(firstCanonical, firstSignature, now); err != nil {
		t.Fatal(err)
	}
	if _, err := secondDirectory.Publish(secondCanonical, secondSignature, now); err != nil {
		t.Fatal(err)
	}
	transport := &directoryTransport{handlers: map[string]http.Handler{"one.example": HTTPHandler(firstDirectory), "two.example": HTTPHandler(secondDirectory)}}
	client, err := NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := client.Lookup(context.Background(), directoryTestIdentityHint(t), now.Add(time.Second))
	if err != nil || len(lookup.Advertisements) != 0 || len(lookup.ForkedNodeIDs) != 1 {
		t.Fatalf("conflicting signed location was auto-selected: lookup=%+v err=%v", lookup, err)
	}
}

func TestFederationRejectsInvalidProviderAdvertisement(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	canonical, signature := signDirectoryAdvertisement(t, directoryTestKey(0x74), "1", now, 0x86)
	signature[0] ^= 1
	bad, err := NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: &rawDirectoryTransport{canonical: canonical, signature: signature}})
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := bad.Lookup(context.Background(), directoryTestIdentityHint(t), now.Add(time.Second))
	if err == nil || !errors.Is(err, ErrNoProvider) || lookup.Providers[0].Code != "advertisement_invalid" {
		t.Fatalf("invalid directory result accepted: lookup=%+v err=%v", lookup, err)
	}
}

func newTestDirectory(t *testing.T) *Directory {
	t.Helper()
	directory, err := OpenDirectory(filepath.Join(t.TempDir(), "directory"))
	if err != nil {
		t.Fatal(err)
	}
	return directory
}

func newFederationTLSServer(t *testing.T, hostname string, directory *Directory, roots *x509.CertPool) (*httptest.Server, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificateTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(int64(len(hostname) + 1)),
		Subject:      pkix.Name{CommonName: hostname},
		DNSNames:     []string{hostname},
		NotBefore:    time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true, IsCA: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, certificateTemplate, certificateTemplate, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots.AddCert(parsed)
	server := httptest.NewUnstartedServer(HTTPHandler(directory))
	server.TLS = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: privateKey}},
		MinVersion:   tls.VersionTLS12,
	}
	server.StartTLS()
	return server, server.Listener.Addr().String()
}

type directoryTransport struct {
	handlers map[string]http.Handler
	failures map[string]bool
}

func (transport *directoryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	host := request.URL.Hostname()
	if transport.failures[host] {
		return nil, errors.New("test transport unavailable")
	}
	handler := transport.handlers[host]
	if handler == nil {
		return nil, errors.New("test provider missing")
	}
	requestCopy := request.Clone(request.Context())
	requestCopy.RemoteAddr = "192.0.2.21:443"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, requestCopy)
	response := recorder.Result()
	response.Request = request
	return response, nil
}

type rawDirectoryTransport struct{ canonical, signature []byte }

func (transport *rawDirectoryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method == http.MethodPost {
		return &http.Response{StatusCode: http.StatusCreated, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: request}, nil
	}
	body, _ := json.Marshal(map[string]any{"advertisements": []map[string]string{{
		"canonical_advertisement": base64.RawURLEncoding.EncodeToString(transport.canonical),
		"signature":               base64.RawURLEncoding.EncodeToString(transport.signature),
	}}})
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header), Request: request}, nil
}
