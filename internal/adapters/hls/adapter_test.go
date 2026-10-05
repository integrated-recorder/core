package hls

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/buildinfo"
)

func TestDescribeIsBundledHLSResolveAdapter(t *testing.T) {
	descriptor := Describe()
	if err := descriptor.Validate(); err != nil {
		t.Fatalf("descriptor validation: %v", err)
	}
	if descriptor.ID != "hls" || descriptor.Name != "HLS" || descriptor.Version != buildinfo.Current().Version || descriptor.ProtocolVersion != adapterproto.Version {
		t.Fatalf("descriptor identity = %#v", descriptor)
	}
	if len(descriptor.Capabilities) != 1 || descriptor.Capabilities[0] != adapterproto.CapabilityResolve {
		t.Fatalf("capabilities = %#v", descriptor.Capabilities)
	}
	if len(descriptor.InputSchema.Fields) != 1 || descriptor.InputSchema.Fields[0].Key != "manifest_url" || !descriptor.InputSchema.Fields[0].Required {
		t.Fatalf("input schema = %#v", descriptor.InputSchema)
	}
	if len(descriptor.ConfigurationSchema.Fields) != 0 || len(descriptor.ResourceTypes) != 0 || len(descriptor.MediaTypes) != 1 || descriptor.MediaTypes[0] != "hls" {
		t.Fatalf("descriptor declares unsupported surfaces: %#v", descriptor)
	}
	encoded, err := json.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	for _, selfClaim := range []string{"trust", "provenance", "publisher", "registry_authority", "reviewed"} {
		if bytes.Contains(encoded, []byte(selfClaim)) {
			t.Fatalf("descriptor self-claims %q: %s", selfClaim, encoded)
		}
	}
}

func TestResolveDirectManifestURLs(t *testing.T) {
	for _, test := range []struct {
		name string
		url  string
		want string
	}{
		{name: "https", url: "https://media.example/live.m3u8", want: "https://media.example/live.m3u8"},
		{name: "http", url: "http://media.example/live.m3u8", want: "http://media.example/live.m3u8"},
		{name: "query and fragment", url: "https://media.example/stream?token=a%2Fb&quality=hd#player", want: "https://media.example/stream?token=a%2Fb&quality=hd"},
		{name: "extensionless", url: "https://media.example/live/channel?id=17", want: "https://media.example/live/channel?id=17"},
	} {
		t.Run(test.name, func(t *testing.T) {
			input, _ := json.Marshal(map[string]string{"manifest_url": test.url})
			media, err := Resolve(input)
			if err != nil {
				t.Fatal(err)
			}
			if media.Type != "hls" || media.ManifestURL != test.want {
				t.Fatalf("media = %#v, want URL %q", media, test.want)
			}
			if len(media.Headers) != 0 || media.RequestPolicy != nil {
				t.Fatalf("generic HLS adapter added transport behavior: %#v", media)
			}
		})
	}
}

func TestResolveRejectsInvalidManifestURL(t *testing.T) {
	inputs := []struct {
		name  string
		input []byte
	}{
		{name: "missing", input: []byte(`{}`)},
		{name: "empty", input: []byte(`{"manifest_url":""}`)},
		{name: "non-http scheme", input: []byte(`{"manifest_url":"file:///tmp/manifest"}`)},
		{name: "no host", input: []byte(`{"manifest_url":"https:///manifest"}`)},
		{name: "userinfo", input: []byte(`{"manifest_url":"https://user:pass@media.example/live"}`)},
		{name: "opaque", input: []byte(`{"manifest_url":"https:media.example/live"}`)},
		{name: "relative", input: []byte(`{"manifest_url":"//media.example/live"}`)},
		{name: "malformed", input: []byte(`{"manifest_url":"https://[::1/live"}`)},
		{name: "control character", input: []byte(`{"manifest_url":"https://media.example/live\u0001"}`)},
		{name: "unknown input", input: []byte(`{"manifest_url":"https://media.example/live","source_url":"https://other.example"}`)},
		{name: "leading whitespace", input: []byte(`{"manifest_url":" https://media.example/live"}`)},
		{name: "oversized", input: mustJSONURL(strings.Repeat("a", MaxManifestURLBytes+1))},
		{name: "invalid UTF-8", input: append([]byte(`{"manifest_url":"https://media.example/`), append([]byte{0xff}, []byte(`"}`)...)...)},
	}
	for _, test := range inputs {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Resolve(test.input); err == nil {
				t.Fatalf("Resolve(%q) succeeded", test.input)
			}
		})
	}
}

func mustJSONURL(value string) []byte {
	encoded, err := json.Marshal(map[string]string{"manifest_url": value})
	if err != nil {
		panic(err)
	}
	return encoded
}

func TestServeSequentialProtocolAndShutdown(t *testing.T) {
	var input bytes.Buffer
	for _, request := range []adapterproto.Request{
		{ProtocolVersion: adapterproto.Version, ID: "describe-1", Method: adapterproto.MethodDescribe},
		{ProtocolVersion: adapterproto.Version, ID: "resolve-2", Method: adapterproto.MethodResolve, Params: json.RawMessage(`{"input":{"manifest_url":"https://media.example/channel?token=abc#fragment"}}`)},
		{ProtocolVersion: adapterproto.Version, ID: "unknown-3", Method: "private-unknown-method"},
		{ProtocolVersion: adapterproto.Version, ID: "shutdown-4", Method: adapterproto.MethodShutdown},
		{ProtocolVersion: adapterproto.Version, ID: "after-shutdown", Method: adapterproto.MethodDescribe},
	} {
		if err := adapterproto.WriteRequest(&input, request); err != nil {
			t.Fatal(err)
		}
	}
	var output bytes.Buffer
	if err := Serve(&input, &output); err != nil {
		t.Fatal(err)
	}

	wire := append([]byte(nil), output.Bytes()...)
	reader := bufio.NewReader(bytes.NewReader(wire))
	responses := make([]adapterproto.Response, 0, 4)
	for {
		response, err := adapterproto.ReadResponse(reader)
		if err != nil {
			break
		}
		responses = append(responses, response)
	}
	if len(responses) != 4 {
		t.Fatalf("response count = %d, want 4; output %q", len(responses), wire)
	}
	for i, wantID := range []string{"describe-1", "resolve-2", "unknown-3", "shutdown-4"} {
		if responses[i].ID != wantID || responses[i].ProtocolVersion != adapterproto.Version {
			t.Fatalf("response[%d] = %#v", i, responses[i])
		}
	}
	var descriptor adapterproto.Descriptor
	if err := json.Unmarshal(responses[0].Result, &descriptor); err != nil || descriptor.ID != "hls" {
		t.Fatalf("describe result = %s, err %v", responses[0].Result, err)
	}
	var media adapterproto.MediaSource
	if err := json.Unmarshal(responses[1].Result, &media); err != nil || media.ManifestURL != "https://media.example/channel?token=abc" {
		t.Fatalf("resolve result = %s, err %v", responses[1].Result, err)
	}
	if responses[2].Error == nil || responses[2].Error.Code != "unsupported_method" || responses[2].Error.Details != nil {
		t.Fatalf("unknown-method response = %#v", responses[2])
	}
	if bytes.Contains(wire, []byte("private-unknown-method")) {
		t.Fatalf("unknown request method leaked in output: %s", wire)
	}
	if !bytes.HasSuffix(wire, []byte("\n")) {
		t.Fatal("protocol output does not end in a newline frame")
	}
}

func TestServeInvalidInputReturnsSafeErrorAndContinues(t *testing.T) {
	var input bytes.Buffer
	for _, request := range []adapterproto.Request{
		{ProtocolVersion: adapterproto.Version, ID: "bad-1", Method: adapterproto.MethodResolve, Params: json.RawMessage(`{"input":{"manifest_url":"file:///secret/path"}}`)},
		{ProtocolVersion: adapterproto.Version, ID: "good-2", Method: adapterproto.MethodResolve, Params: json.RawMessage(`{"input":{"manifest_url":"https://media.example/manifest"}}`)},
		{ProtocolVersion: adapterproto.Version, ID: "stop-3", Method: adapterproto.MethodShutdown},
	} {
		if err := adapterproto.WriteRequest(&input, request); err != nil {
			t.Fatal(err)
		}
	}
	var output bytes.Buffer
	if err := Serve(&input, &output); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(bytes.NewReader(output.Bytes()))
	first, err := adapterproto.ReadResponse(reader)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != "bad-1" || first.Error == nil || first.Error.Code != "invalid_input" || first.Error.Message != "manifest URL input is invalid" {
		t.Fatalf("invalid-input response = %#v", first)
	}
	errorBytes, err := json.Marshal(first.Error)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(errorBytes), "secret") || strings.Contains(string(errorBytes), "file:") {
		t.Fatalf("invalid-input error leaked source input: %#v", first.Error)
	}
	second, err := adapterproto.ReadResponse(reader)
	if err != nil || second.ID != "good-2" || second.Error != nil {
		t.Fatalf("subsequent resolve response = %#v, err %v", second, err)
	}
}
