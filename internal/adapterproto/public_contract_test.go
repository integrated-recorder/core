package adapterproto

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func protocolRepoRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate protocol test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
}

func readGolden(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join(protocolRepoRoot(t), "protocol", "adapter-v1", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	return []byte(strings.TrimSpace(string(data)))
}

func TestPublicProtocolV1GoldenFrames(t *testing.T) {
	requestIDs := map[string]string{
		"describe.request.json": "1", "resolve.request.json": "2",
		"watch-offline.request.json": "3", "watch-live.request.json": "4",
		"metadata.request.json": "5", "refresh.request.json": "6",
		"resource-list.request.json": "7", "workflow-begin.request.json": "8",
		"workflow-continue.request.json": "9", "shutdown.request.json": "10",
	}
	responseIDs := map[string]string{
		"describe.response.json": "1", "resolve.response.json": "2",
		"watch-offline.response.json": "3", "watch-live.response.json": "4",
		"metadata.response.json": "5", "refresh.response.json": "6",
		"resource-list.response.json": "7", "workflow-begin.response.json": "8",
		"workflow-continue.response.json": "9", "shutdown.response.json": "10",
	}
	for filename, wantID := range requestIDs {
		t.Run(filename, func(t *testing.T) {
			frame, err := ParseFrame(readGolden(t, filename))
			if err != nil || frame.Kind != FrameTypeRequest || frame.Request == nil {
				t.Fatalf("request fixture rejected: %#v, %v", frame, err)
			}
			if frame.Request.ID != wantID || frame.Request.ProtocolVersion != Version || frame.Request.Method == "" {
				t.Fatalf("request fixture = %#v", frame.Request)
			}
		})
	}
	for filename, wantID := range responseIDs {
		t.Run(filename, func(t *testing.T) {
			frame, err := ParseFrame(readGolden(t, filename))
			if err != nil || frame.Kind != FrameTypeResponse || frame.Response == nil {
				t.Fatalf("response fixture rejected: %#v, %v", frame, err)
			}
			if frame.Response.ID != wantID || frame.Response.ProtocolVersion != Version || frame.Response.Error != nil {
				t.Fatalf("response fixture = %#v", frame.Response)
			}
		})
	}

	t.Run("descriptor", func(t *testing.T) {
		var descriptor Descriptor
		if err := json.Unmarshal(resultFromGolden(t, "describe.response.json"), &descriptor); err != nil {
			t.Fatal(err)
		}
		if err := descriptor.Validate(); err != nil {
			t.Fatalf("public descriptor is invalid: %v", err)
		}
	})
	t.Run("resolve media", func(t *testing.T) {
		var media MediaSource
		if err := json.Unmarshal(resultFromGolden(t, "resolve.response.json"), &media); err != nil {
			t.Fatal(err)
		}
		if err := ValidateMediaSource(media, []string{"hls"}); err != nil {
			t.Fatalf("resolve media is invalid: %v", err)
		}
	})
	t.Run("watch results", func(t *testing.T) {
		for _, name := range []string{"watch-offline.response.json", "watch-live.response.json"} {
			var result WatchCheckResult
			if err := json.Unmarshal(resultFromGolden(t, name), &result); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if err := result.Validate([]string{"hls"}); err != nil {
				t.Fatalf("%s is invalid: %v", name, err)
			}
		}
	})
	t.Run("metadata", func(t *testing.T) {
		var result MetadataResult
		if err := json.Unmarshal(resultFromGolden(t, "metadata.response.json"), &result); err != nil {
			t.Fatal(err)
		}
		if result.Metadata.Description == nil || *result.Metadata.Description != "" {
			t.Fatal("golden must preserve known-empty description semantics")
		}
		if err := result.Validate(); err != nil {
			t.Fatalf("metadata result is invalid: %v", err)
		}
	})
	t.Run("refresh", func(t *testing.T) {
		var result RefreshResult
		if err := json.Unmarshal(resultFromGolden(t, "refresh.response.json"), &result); err != nil {
			t.Fatal(err)
		}
		if err := ValidateMediaSource(result.Media, []string{"hls"}); err != nil {
			t.Fatalf("refresh media is invalid: %v", err)
		}
	})
	t.Run("resource page", func(t *testing.T) {
		var page ResourcePage
		if err := json.Unmarshal(resultFromGolden(t, "resource-list.response.json"), &page); err != nil {
			t.Fatal(err)
		}
		if page.Items == nil || len(page.Items) != 1 {
			t.Fatalf("resource page items = %#v", page.Items)
		}
		var descriptor Descriptor
		if err := json.Unmarshal(resultFromGolden(t, "describe.response.json"), &descriptor); err != nil {
			t.Fatal(err)
		}
		for i := range page.Items {
			if err := ValidateResourceRefForDescriptor(descriptor, &page.Items[i].ResourceRef); err != nil {
				t.Fatalf("resource is invalid: %v", err)
			}
		}
	})
	t.Run("workflow results", func(t *testing.T) {
		for _, fixture := range []struct {
			name  string
			state string
		}{{"workflow-begin.response.json", "configuration_required"}, {"workflow-continue.response.json", "resolved"}} {
			var result ResolveWorkflowResult
			if err := json.Unmarshal(resultFromGolden(t, fixture.name), &result); err != nil {
				t.Fatalf("%s: %v", fixture.name, err)
			}
			if result.State != fixture.state || result.WorkflowID != "core-generated-opaque-id" {
				t.Fatalf("%s = %#v", fixture.name, result)
			}
			if err := ValidateWorkflowResult(result, "core-generated-opaque-id", []string{"hls"}); err != nil {
				t.Fatalf("%s is invalid: %v", fixture.name, err)
			}
		}
	})
	t.Run("error fields", func(t *testing.T) {
		frame, err := ParseFrame(readGolden(t, "error-unsupported-method.json"))
		if err != nil || frame.Response == nil || frame.Response.Error == nil {
			t.Fatalf("error response frame = %#v, %v", frame, err)
		}
		if frame.Response.ID != "unknown-1" || frame.Response.ProtocolVersion != Version || frame.Response.Error.Code == "" {
			t.Fatal("structured error code must be nonempty")
		}
	})
}

func resultFromGolden(t *testing.T, name string) []byte {
	t.Helper()
	frame, err := ParseFrame(readGolden(t, name))
	if err != nil || frame.Response == nil || len(frame.Response.Result) == 0 {
		t.Fatalf("read response result %s: %v", name, err)
	}
	return frame.Response.Result
}

func TestProtocolConstantsAreListedInNormativeSpec(t *testing.T) {
	root := protocolRepoRoot(t)
	spec, err := os.ReadFile(filepath.Join(root, "docs", "ADAPTER_PROTOCOL_V1.md"))
	if err != nil {
		t.Fatal(err)
	}
	var capabilities, methods []string
	for _, name := range []string{"protocol.go", "timed_events.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, "internal", "adapterproto", name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			group, ok := declaration.(*ast.GenDecl)
			if !ok || group.Tok != token.CONST {
				continue
			}
			for _, spec := range group.Specs {
				valueSpec := spec.(*ast.ValueSpec)
				for index, name := range valueSpec.Names {
					if index >= len(valueSpec.Values) {
						continue
					}
					literal, ok := valueSpec.Values[index].(*ast.BasicLit)
					if !ok || literal.Kind != token.STRING {
						continue
					}
					value, unquoteErr := strconv.Unquote(literal.Value)
					if unquoteErr != nil {
						t.Fatal(unquoteErr)
					}
					switch {
					case strings.HasPrefix(name.Name, "Capability"):
						capabilities = append(capabilities, value)
					case strings.HasPrefix(name.Name, "Method"):
						methods = append(methods, value)
					}
				}
			}
		}
	}
	assertSpecInventory(t, string(spec), "protocol-v1-capabilities", capabilities)
	assertSpecInventory(t, string(spec), "protocol-v1-methods", methods)
}

func assertSpecInventory(t *testing.T, spec, marker string, want []string) {
	t.Helper()
	start, end := "<!-- "+marker+":start -->", "<!-- "+marker+":end -->"
	startIndex := strings.Index(spec, start)
	endIndex := strings.Index(spec, end)
	if startIndex < 0 || endIndex <= startIndex {
		t.Fatalf("normative inventory markers %q are missing or reversed", marker)
	}
	section := spec[startIndex+len(start) : endIndex]
	listed := map[string]bool{}
	wanted := make(map[string]bool, len(want))
	for _, value := range want {
		wanted[value] = true
	}
	for _, line := range strings.Split(section, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			if listed[line] {
				t.Errorf("%s inventory lists %q more than once", marker, line)
			}
			if !wanted[line] {
				t.Errorf("%s inventory contains stale or unknown entry %q", marker, line)
			}
			listed[line] = true
		}
	}
	for _, value := range want {
		if !listed[value] {
			t.Errorf("%s inventory omits production constant %q", marker, value)
		}
	}
}
