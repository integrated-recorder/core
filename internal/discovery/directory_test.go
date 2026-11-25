package discovery

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/discoveryprotocol"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

func TestDirectoryPublishLookupPersistenceReplayAndExpiry(t *testing.T) {
	root := filepath.Join(t.TempDir(), "directory")
	directory, err := OpenDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	privateKey := directoryTestKey(0x11)
	first, firstSignature := signDirectoryAdvertisement(t, privateKey, "1", now, 0x31)
	if already, err := directory.Publish(first, firstSignature, now); err != nil || already {
		t.Fatalf("first publish already=%v err=%v", already, err)
	}
	results, err := directory.Lookup(directoryTestIdentityHint(t), now.Add(time.Second))
	if err != nil || len(results) != 1 || !bytes.Equal(results[0].Canonical, first) {
		t.Fatalf("lookup results=%+v err=%v", results, err)
	}
	statePath := filepath.Join(root, directoryStateFile)
	if mode := fileModeAt(t, statePath); mode != directoryFileMode {
		t.Fatalf("state mode=%#o", mode)
	}
	if mode := fileModeAt(t, root); mode&0077 != 0 {
		t.Fatalf("directory mode=%#o", mode)
	}

	reopened, err := OpenDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	if already, err := reopened.Publish(first, firstSignature, now.Add(time.Second)); err != nil || !already {
		t.Fatalf("idempotent publish already=%v err=%v", already, err)
	}
	second, secondSignature := signDirectoryAdvertisement(t, privateKey, "2", now.Add(time.Minute), 0x32)
	if already, err := reopened.Publish(second, secondSignature, now.Add(time.Minute)); err != nil || already {
		t.Fatalf("successor publish already=%v err=%v", already, err)
	}
	if _, err := reopened.Publish(first, firstSignature, now.Add(time.Minute)); !errors.Is(err, ErrDirectoryReplay) {
		t.Fatalf("old signed sequence replay accepted: %v", err)
	}
	results, err = reopened.Lookup(directoryTestIdentityHint(t), now.Add(2*time.Minute))
	if err != nil || len(results) != 1 || results[0].Sequence != "2" || !bytes.Equal(results[0].Canonical, second) {
		t.Fatalf("successor lookup=%+v err=%v", results, err)
	}
	results, err = reopened.Lookup(directoryTestIdentityHint(t), now.Add(11*time.Minute))
	if err != nil || len(results) != 0 {
		t.Fatalf("expired ad remained discoverable: count=%d err=%v", len(results), err)
	}
}

func TestDirectoryEquivocationPersistsEvidenceAndFreezesNode(t *testing.T) {
	root := filepath.Join(t.TempDir(), "directory")
	directory, err := OpenDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	privateKey := directoryTestKey(0x22)
	first, firstSignature := signDirectoryAdvertisement(t, privateKey, "4", now, 0x41)
	second, secondSignature := signDirectoryAdvertisement(t, privateKey, "4", now, 0x42)
	if _, err := directory.Publish(first, firstSignature, now); err != nil {
		t.Fatal(err)
	}
	if _, err := directory.Publish(second, secondSignature, now); !errors.Is(err, ErrEquivocation) {
		t.Fatalf("same-sequence signed equivocation error=%v", err)
	}
	reopened, err := OpenDirectory(root)
	if err != nil {
		t.Fatalf("fork evidence did not survive restart: %v", err)
	}
	results, err := reopened.Lookup(directoryTestIdentityHint(t), now.Add(time.Second))
	if err != nil || len(results) != 0 {
		t.Fatalf("frozen Node remained discoverable: %+v err=%v", results, err)
	}
	third, thirdSignature := signDirectoryAdvertisement(t, privateKey, "5", now.Add(time.Minute), 0x43)
	if _, err := reopened.Publish(third, thirdSignature, now.Add(time.Minute)); !errors.Is(err, ErrNodeFrozen) {
		t.Fatalf("frozen Node accepted a later advertisement: %v", err)
	}
	state, err := reopened.readLocked()
	if err != nil || len(state.Nodes) != 1 || !state.Nodes[0].Frozen || len(state.Nodes[0].ForkEvidence) != 2 {
		t.Fatalf("fork state=%+v err=%v", state, err)
	}
}

func TestDirectoryRejectsInvalidSignatureAndCorruptDurableState(t *testing.T) {
	root := filepath.Join(t.TempDir(), "directory")
	directory, err := OpenDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	canonical, signature := signDirectoryAdvertisement(t, directoryTestKey(0x33), "1", now, 0x51)
	signature[0] ^= 1
	if _, err := directory.Publish(canonical, signature, now); !errors.Is(err, discoveryprotocol.ErrInvalid) {
		t.Fatalf("bad signature publish error=%v", err)
	}
	if _, err := directory.Lookup(directoryTestIdentityHint(t), now); err != nil {
		t.Fatalf("invalid publish changed state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, directoryStateFile), []byte(`{"schema_version":1,"nodes":[{}]}`), directoryFileMode); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDirectory(root); !errors.Is(err, ErrDirectoryInvalid) {
		t.Fatalf("malformed durable state opened: %v", err)
	}
}

func TestDirectoryHTTPHandlerPublishesAndReturnsOnlyVerifiedActiveRecords(t *testing.T) {
	directory, err := OpenDirectory(filepath.Join(t.TempDir(), "directory"))
	if err != nil {
		t.Fatal(err)
	}
	handler := HTTPHandler(directory)
	now := time.Now().UTC().Truncate(time.Second)
	canonical, signature := signDirectoryAdvertisement(t, directoryTestKey(0x44), "1", now, 0x61)
	body, err := json.Marshal(signedValue{Canonical: base64.RawURLEncoding.EncodeToString(canonical), Signature: base64.RawURLEncoding.EncodeToString(signature)})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/advertisements", bytes.NewReader(body))
	request.RemoteAddr = "192.0.2.44:1234"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated || response.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("publish status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/advertisements?identity_hint="+directoryTestIdentityHint(t), nil)
	request.RemoteAddr = "192.0.2.44:1234"
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("lookup status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	var result struct {
		Advertisements []signedValue `json:"advertisements"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || len(result.Advertisements) != 1 {
		t.Fatalf("lookup response=%s err=%v", response.Body.String(), err)
	}
	decoded, _ := base64.RawURLEncoding.DecodeString(result.Advertisements[0].Canonical)
	decodedSignature, _ := base64.RawURLEncoding.DecodeString(result.Advertisements[0].Signature)
	if _, err := discoveryprotocol.Verify(decoded, decodedSignature, now.Add(time.Second)); err != nil {
		t.Fatalf("HTTP lookup returned invalid signed data: %v", err)
	}
}

func TestDirectoryHTTPIssuesPublicIdentityHistoryMirrorWithoutFreshnessClaims(t *testing.T) {
	root := filepath.Join(t.TempDir(), "directory")
	directory, err := OpenDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	bundle := signedIdentityHistoryBundle(t, 0x44, 0x71)
	handler := HTTPHandler(directory)
	body, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPut, "/v1/identity-histories/"+bundle.IdentityID, bytes.NewReader(body))
	request.RemoteAddr = "192.0.2.91:443"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("identity history publish status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "/v1/identity-histories/"+bundle.IdentityID, nil)
	request.RemoteAddr = "192.0.2.91:443"
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" || strings.Contains(response.Body.String(), "private_key") {
		t.Fatalf("identity history lookup status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	var returned globalidentity.HistoryBundle
	if err := json.Unmarshal(response.Body.Bytes(), &returned); err != nil || returned.IdentityID != bundle.IdentityID {
		t.Fatalf("identity history response=%s err=%v", response.Body.String(), err)
	}
	// A competing signed genesis for the same ID freezes the history and is
	// never treated as a successful current-history publication.
	competing := signedIdentityHistoryBundle(t, 0x44, 0x72)
	competingBody, err := json.Marshal(competing)
	if err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest(http.MethodPut, "/v1/identity-histories/"+bundle.IdentityID, bytes.NewReader(competingBody))
	request.RemoteAddr = "192.0.2.91:443"
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("competing valid history status=%d body=%s", response.Code, response.Body.String())
	}
	reopened, err := OpenDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := reopened.history.AuthorityProjection(bundle.IdentityID)
	if err != nil || !projection.Frozen {
		t.Fatalf("frozen fork was not durable: projection=%+v err=%v", projection, err)
	}
}

func TestDirectorySerializesConcurrentIndependentInstances(t *testing.T) {
	root := filepath.Join(t.TempDir(), "directory")
	directories := make([]*Directory, 6)
	for i := range directories {
		directory, err := OpenDirectory(root)
		if err != nil {
			t.Fatal(err)
		}
		directories[i] = directory
	}
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	type publication struct{ canonical, signature []byte }
	items := make([]publication, len(directories))
	for i := range items {
		items[i].canonical, items[i].signature = signDirectoryAdvertisement(t, directoryTestKey(byte(0x60+i)), "1", now, byte(0x70+i))
	}
	start := make(chan struct{})
	var wait sync.WaitGroup
	errs := make(chan error, len(directories))
	for i := range directories {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			<-start
			_, err := directories[i].Publish(items[i].canonical, items[i].signature, now)
			errs <- err
		}(i)
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	results, err := directories[0].Lookup(directoryTestIdentityHint(t), now.Add(time.Second))
	if err != nil || len(results) != len(directories) {
		t.Fatalf("concurrent publications=%d err=%v", len(results), err)
	}
}

func signDirectoryAdvertisement(t *testing.T, privateKey ed25519.PrivateKey, sequence string, issued time.Time, addressByte byte) ([]byte, []byte) {
	t.Helper()
	publicKey := privateKey.Public().(ed25519.PublicKey)
	nodeID, err := identityprotocol.NodeID(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	identityHint := directoryTestIdentityHint(t)
	headHash := "sha256:" + strings.Repeat("a", 64)
	recipient, err := ecdh.X25519().NewPrivateKey(bytes.Repeat([]byte{0x63}, 32))
	if err != nil {
		t.Fatal(err)
	}
	addressEnvelope, err := discoveryprotocol.SealAddressEnvelope(recipient.PublicKey().Bytes(), discoveryprotocol.AddressContext{
		GlobalIdentityHint: identityHint, IdentityHeadHash: headHash, NodeID: nodeID,
	}, []byte(fmt.Sprintf(`{"addresses":["wss://directory-test-%d.example/signal"]}`, addressByte)))
	if err != nil {
		t.Fatal(err)
	}
	advertisement := discoveryprotocol.Advertisement{
		AdvertisementVersion: discoveryprotocol.Version,
		GlobalIdentityHint:   identityHint,
		IdentityHeadHash:     headHash,
		NodeID:               nodeID,
		NodePublicKey:        base64.RawURLEncoding.EncodeToString(publicKey),
		Sequence:             sequence,
		IssuedAt:             issued.Format(time.RFC3339),
		ExpiresAt:            issued.Add(5 * time.Minute).Format(time.RFC3339),
		AddressEnvelope:      addressEnvelope,
		Capabilities:         []string{"webrtc-datachannel/1"},
		RelayIDs:             []string{},
	}
	canonical, signature, err := discoveryprotocol.Sign(advertisement, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, signature
}

func directoryTestKey(first byte) ed25519.PrivateKey {
	seed := bytes.Repeat([]byte{first}, ed25519.SeedSize)
	return ed25519.NewKeyFromSeed(seed)
}

func directoryTestIdentityHint(t *testing.T) string {
	t.Helper()
	hint, err := discoveryprotocol.GlobalIdentityHint("gi_AAAAAAAAAAAAAAAAAAAAAA")
	if err != nil {
		t.Fatal(err)
	}
	return hint
}

func fileModeAt(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}
