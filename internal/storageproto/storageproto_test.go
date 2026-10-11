package storageproto

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestDescriptorValidationAndFingerprint(t *testing.T) {
	descriptor := fixtureDescriptor()
	if err := ValidateDescriptor(descriptor); err != nil {
		t.Fatal(err)
	}
	fingerprint, err := Fingerprint(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	second := fixtureDescriptor()
	second.Capabilities[0], second.Capabilities[1] = second.Capabilities[1], second.Capabilities[0]
	other, err := Fingerprint(second)
	if err != nil {
		t.Fatal(err)
	}
	if fingerprint != other {
		t.Fatal("capability ordering changed semantic fingerprint")
	}
	second.ProtocolVersion++
	if err := ValidateDescriptor(second); err == nil {
		t.Fatal("unsupported protocol version accepted")
	}
}

func TestKeyValidation(t *testing.T) {
	valid := []string{"recordings/abc/manifest.json", "a", "üñîcode/key"}
	for _, key := range valid {
		if err := ValidateKey(key); err != nil {
			t.Errorf("key %q rejected: %v", key, err)
		}
	}
	invalid := []string{"", "/absolute", "a//b", "a/./b", "a/../b", `a\b`, "a\x00b", strings.Repeat("x", 1025)}
	for _, key := range invalid {
		if err := ValidateKey(key); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("key %q got %v", key, err)
		}
	}
	if err := ValidatePrefix(""); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePrefix("recordings/"); err != nil {
		t.Fatalf("trailing slash prefix rejected: %v", err)
	}
	if err := ValidatePrefix("../x"); err == nil {
		t.Fatal("unsafe prefix accepted")
	}
	if err := ValidatePrefix("recordings//"); err == nil {
		t.Fatal("repeated trailing slash accepted")
	}
}

func TestConfigValidation(t *testing.T) {
	if err := ValidateConfig(Config{Values: map[string]json.RawMessage{"bucket": json.RawMessage(`"archive"`)}, Secrets: map[string]string{"secret": "credential"}}); err != nil {
		t.Fatal(err)
	}
	for _, config := range []Config{
		{Values: map[string]json.RawMessage{"bad/key": json.RawMessage(`1`)}},
		{Values: map[string]json.RawMessage{"x": json.RawMessage(`{`)}},
		{Secrets: map[string]string{"x": strings.Repeat("s", (16<<10)+1)}},
	} {
		if err := ValidateConfig(config); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
}

func TestTokenFileMustBePrivateRegularFile(t *testing.T) {
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte("0123456789abcdef0123456789abcdef"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readToken(token); err != nil {
		t.Fatalf("private token file rejected: %v", err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(token, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readToken(link); err == nil {
		t.Fatal("token symlink accepted")
	}
	if err := os.Chmod(token, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readToken(token); err == nil {
		t.Fatal("world-readable token accepted")
	}
}

func TestGoldenStorageProtocolVectors(t *testing.T) {
	root := filepath.Join("..", "..", "protocol", "storage-provider-v1")
	data, err := os.ReadFile(filepath.Join(root, "descriptor.response.json"))
	if err != nil {
		t.Fatal(err)
	}
	var descriptor Descriptor
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&descriptor); err != nil {
		t.Fatal(err)
	}
	if err = ValidateDescriptor(descriptor); err != nil {
		t.Fatal(err)
	}
	configData, err := os.ReadFile(filepath.Join(root, "config.request.json"))
	if err != nil {
		t.Fatal(err)
	}
	var config Config
	decoder = json.NewDecoder(bytes.NewReader(configData))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&config); err != nil {
		t.Fatal(err)
	}
	if err = ValidateConfig(config); err != nil {
		t.Fatal(err)
	}
	probeData, err := os.ReadFile(filepath.Join(root, "probe.response.json"))
	if err != nil {
		t.Fatal(err)
	}
	var probe struct {
		Ready bool `json:"ready"`
	}
	if err = json.Unmarshal(probeData, &probe); err != nil || !probe.Ready {
		t.Fatalf("invalid probe vector: %v", err)
	}
	errorData, err := os.ReadFile(filepath.Join(root, "error.not_found.json"))
	if err != nil {
		t.Fatal(err)
	}
	var wire wireError
	if err = json.Unmarshal(errorData, &wire); err != nil || wire.Error.Code != "not_found" {
		t.Fatalf("invalid error vector: %v", err)
	}
	objectData, err := os.ReadFile(filepath.Join(root, "object.info.json"))
	if err != nil {
		t.Fatal(err)
	}
	var object ObjectInfo
	if err = json.Unmarshal(objectData, &object); err != nil || object.Size != 3 || !validDigest(object.SHA256) {
		t.Fatalf("invalid object vector: %v", err)
	}
	rangeData, err := os.ReadFile(filepath.Join(root, "range.headers.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(rangeData, []byte("Content-Range: bytes 0-2/3")) || !bytes.Contains(rangeData, []byte(object.SHA256)) {
		t.Fatal("invalid range vector")
	}
	var page ListPage
	pageData, err := os.ReadFile(filepath.Join(root, "list.response.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(pageData, &page); err != nil {
		t.Fatal(err)
	}
	if err = validatePage(page, "recordings/", "", 2); err != nil {
		t.Fatal(err)
	}
}

func TestConformanceRunnerUsesExternalExecutable(t *testing.T) {
	if testing.Short() {
		t.Skip("builds an external provider executable")
	}
	binary := buildFixture(t)
	report, err := RunConformance(context.Background(), binary, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Passed || report.ProviderID != "fixture-storage" || len(report.Checks) < 10 {
		t.Fatalf("incomplete report: %#v", report)
	}
	for _, check := range report.Checks {
		if !check.Pass {
			t.Errorf("conformance check failed: %s", check.Name)
		}
	}
}

func TestConformanceCommandJSONOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("builds CLI and provider child processes")
	}
	provider := buildFixture(t)
	cli := filepath.Join(t.TempDir(), "storage-provider-conformance")
	build := exec.Command("go", "build", "-trimpath", "-o", cli, "../../cmd/storage-provider-conformance")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build conformance CLI: %v (%s)", err, output)
	}
	command := exec.Command(cli, "--binary", provider, "--json")
	stdout, err := command.Output()
	if err != nil {
		t.Fatalf("run conformance CLI: %v", err)
	}
	var report ConformanceReport
	if err = json.Unmarshal(stdout, &report); err != nil {
		t.Fatalf("CLI stdout was not JSON only: %v", err)
	}
	if !report.Passed || report.ProviderID != "fixture-storage" {
		t.Fatalf("CLI did not pass fixture: %#v", report)
	}
}

func TestSubprocessAuthenticationAndBoundedFrames(t *testing.T) {
	if testing.Short() {
		t.Skip("builds an external provider executable")
	}
	binary := buildFixture(t)
	private, err := os.MkdirTemp("/tmp", "ir-spv1-auth-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(private)
	private, err = filepath.EvalSymlinks(private)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.Chmod(private, 0700)
	tokenPath := filepath.Join(private, "token")
	if err := os.WriteFile(tokenPath, []byte("0123456789abcdef0123456789abcdef"), 0600); err != nil {
		t.Fatal(err)
	}
	client, err := Start(context.Background(), StartOptions{Binary: binary, SocketPath: filepath.Join(private, "provider.sock"), TokenFile: tokenPath})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	wrongToken := filepath.Join(private, "wrong-token")
	if err = os.WriteFile(wrongToken, []byte("fedcba9876543210fedcba9876543210"), 0600); err != nil {
		t.Fatal(err)
	}
	unauthorized, err := NewClient(filepath.Join(private, "provider.sock"), wrongToken)
	if err != nil {
		t.Fatal(err)
	}
	_, err = unauthorized.Describe(context.Background())
	_ = unauthorized.Close()
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Code != "unauthorized" {
		t.Fatalf("wrong token response was not safely rejected: %v", err)
	}

	oversized := bytes.NewReader(make([]byte, MaxConfigBytes+1))
	req, err := client.request(context.Background(), http.MethodPut, "/v1/config", oversized)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := client.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	err = checkStatus(response, http.StatusNoContent)
	_ = response.Body.Close()
	if !errors.As(err, &remote) || remote.Code != "invalid_size" {
		t.Fatalf("oversized config body was not bounded: %v", err)
	}

	malformed := bytes.NewReader([]byte("{"))
	req, err = client.request(context.Background(), http.MethodPut, "/v1/config", malformed)
	if err != nil {
		t.Fatal(err)
	}
	response, err = client.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	err = checkStatus(response, http.StatusNoContent)
	_ = response.Body.Close()
	if !errors.As(err, &remote) || remote.Code != "invalid_request" {
		t.Fatalf("malformed frame was not rejected: %v", err)
	}

	req, err = client.request(context.Background(), http.MethodGet, "/v1/private", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = client.http.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxControlFrameBytes+1))
	_ = response.Body.Close()
	var notFound wireError
	if err != nil || response.StatusCode != http.StatusNotFound || json.Unmarshal(body, &notFound) != nil || notFound.Error.Code != "not_found" {
		t.Fatalf("unknown route did not return bounded error: status=%d", response.StatusCode)
	}
}

func TestListResponseIsByteBoundedAndPreservesPagination(t *testing.T) {
	items := make([]ObjectEntry, 708)
	for i := range items {
		items[i] = ObjectEntry{Key: fmt.Sprintf("recordings/%032x/archive/v2/manifests/%020d-%s.json", 1, i, strings.Repeat("x", 48)), Size: int64(i)}
	}
	fullPage := ListPage{Items: items}
	encoded, err := json.Marshal(fullPage)
	if err != nil || len(encoded) <= MaxControlFrameBytes {
		t.Fatalf("fixture response bytes=%d, err=%v; want oversized response", len(encoded), err)
	}
	provider := &largeListProvider{items: items}
	handler := providerHandler(provider, []byte("test-private-token"), 0)
	var collected []string
	cursor := ""
	for pageNumber := 0; pageNumber < len(items); pageNumber++ {
		query := url.Values{"prefix": []string{"recordings/"}, "limit": []string{"1000"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		request := httptest.NewRequest(http.MethodGet, "/v1/list?"+query.Encode(), nil)
		request.Header.Set("Authorization", "Bearer test-private-token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || response.Body.Len() > MaxControlFrameBytes {
			t.Fatalf("page %d status=%d bytes=%d", pageNumber, response.Code, response.Body.Len())
		}
		var page ListPage
		if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
			t.Fatalf("decode bounded page %d: %v", pageNumber, err)
		}
		if err := validatePage(page, "recordings/", cursor, 1000); err != nil {
			t.Fatalf("bounded page %d is invalid: %v", pageNumber, err)
		}
		if len(page.Items) == 0 {
			t.Fatalf("bounded page %d is empty before enumeration completed", pageNumber)
		}
		for _, item := range page.Items {
			collected = append(collected, item.Key)
		}
		if page.NextCursor == "" {
			break
		}
		if page.NextCursor != page.Items[len(page.Items)-1].Key {
			t.Fatalf("page %d cursor=%q does not equal its final key", pageNumber, page.NextCursor)
		}
		cursor = page.NextCursor
	}
	want := make([]string, len(items))
	for i := range items {
		want[i] = items[i].Key
	}
	if !reflect.DeepEqual(collected, want) {
		t.Fatalf("bounded pagination changed object enumeration: got=%d want=%d", len(collected), len(want))
	}
}

func TestLegacyPlainTextGatewayErrorIsTypedForListFallback(t *testing.T) {
	response := httptest.NewRecorder()
	response.Header().Set("Content-Type", "text/plain; charset=utf-8")
	response.WriteHeader(http.StatusBadGateway)
	_, _ = response.Write([]byte("provider response invalid\n"))
	if err := checkStatus(response.Result(), http.StatusOK); !errors.Is(err, ErrUnframedResponse) {
		t.Fatalf("legacy unframed gateway response error=%v, want ErrUnframedResponse", err)
	}
}

type largeListProvider struct {
	Provider
	items []ObjectEntry
}

func (p *largeListProvider) Descriptor() Descriptor { return fixtureDescriptor() }

func (p *largeListProvider) List(_ context.Context, prefix, cursor string, limit int) (ListPage, error) {
	start := 0
	for start < len(p.items) && p.items[start].Key <= cursor {
		start++
	}
	end := min(start+limit, len(p.items))
	page := ListPage{Items: append([]ObjectEntry{}, p.items[start:end]...)}
	if end < len(p.items) && len(page.Items) > 0 {
		page.NextCursor = page.Items[len(page.Items)-1].Key
	}
	return page, nil
}

func buildFixture(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "integrated-recorder-storage-fixture")
	command := exec.Command("go", "build", "-trimpath", "-o", binary, "./testdata/provider")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build provider fixture: %v (%s)", err, output)
	}
	return binary
}

func fixtureDescriptor() Descriptor {
	return Descriptor{ProtocolVersion: Version, ID: "fixture-storage", Name: "Fixture Storage", Version: "1.0.0", ConfigurationSchema: Schema{Fields: []Field{}}, Capabilities: []string{CapabilityRead, CapabilityWrite, CapabilityStat, CapabilityList, CapabilityDelete, CapabilityRangeRead, CapabilityAtomicReplace}}
}
