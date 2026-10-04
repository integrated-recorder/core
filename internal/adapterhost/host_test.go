package adapterhost

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/interaction"
	"github.com/integrated-recorder/core/internal/pluginconfig"
)

func TestMain(m *testing.M) {
	if os.Getenv("IR_ADAPTER_HELPER") == "1" {
		os.Exit(runHelperAdapter())
	}
	os.Exit(m.Run())
}

func TestEffectiveConfigurationOmitsDormantStoredValuesAndSecrets(t *testing.T) {
	schema := adapterproto.Schema{Fields: []adapterproto.Field{
		{Key: "mode", Control: "select", Label: "Mode", Default: json.RawMessage(`"basic"`), Options: []adapterproto.Option{{Value: "basic", Label: "Basic"}, {Value: "advanced", Label: "Advanced"}}},
		{Key: "conditional_value", Control: "text", Label: "Conditional", VisibleWhen: json.RawMessage(`{"field":"mode","equals":"advanced"}`)},
		{Key: "conditional_secret", Control: "secret", Label: "Conditional secret", VisibleWhen: json.RawMessage(`{"field":"mode","equals":"advanced"}`)},
	}}
	effective := pluginconfig.EffectiveDocument{
		EffectiveValues:  map[string]json.RawMessage{"mode": json.RawMessage(`"basic"`), "conditional_value": json.RawMessage(`"stale-value"`)},
		EffectiveSecrets: map[string]string{"conditional_secret": "stale-secret"},
	}
	values, secrets, err := effectiveConfiguration(schema, effective, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := values["conditional_value"]; exists {
		t.Fatal("dormant stored value was sent to adapter")
	}
	if _, exists := secrets["conditional_secret"]; exists {
		t.Fatal("dormant stored secret was sent to adapter")
	}
}

func runHelperAdapter() int {
	mode := os.Getenv("IR_ADAPTER_MODE")
	reader := bufio.NewReader(os.Stdin)
	for {
		request, err := adapterproto.ReadRequest(reader)
		if err == io.EOF {
			return 0
		}
		if err != nil {
			return 2
		}
		if mode == "hang" && request.Method == adapterproto.MethodDescribe {
			time.Sleep(30 * time.Second)
			return 3
		}
		if mode == "badjson" {
			_, _ = io.WriteString(os.Stdout, "not-json\n")
			return 0
		}
		if mode == "mismatch" {
			response, _ := adapterproto.Success("wrong-id", map[string]any{})
			_ = adapterproto.WriteResponse(os.Stdout, response)
			return 0
		}
		var response adapterproto.Response
		switch request.Method {
		case adapterproto.MethodDescribe:
			protocolVersion := adapterproto.Version
			if mode == "incompatible" {
				protocolVersion = adapterproto.Version + 1
			}
			version := os.Getenv("IR_ADAPTER_VERSION")
			if version == "" {
				version = "1"
			}
			capabilities := []string{"resolve"}
			configurationSchema := adapterproto.Schema{Fields: []adapterproto.Field{}}
			resourceTypes := []adapterproto.ResourceType{}
			inputSchema := adapterproto.Schema{Fields: []adapterproto.Field{{Key: "manifest_url", Control: "text", Label: "Manifest URL", Required: true}}}
			variant := os.Getenv("IR_ADAPTER_VARIANT")
			if mode == "refresh" {
				capabilities = append(capabilities, adapterproto.CapabilityRefresh)
			}
			if mode == "metadata" {
				capabilities = append(capabilities, adapterproto.CapabilityMetadata)
				configurationSchema.Fields = []adapterproto.Field{
					{Key: "label", Control: "text", Label: "Label", Inherit: true},
					{Key: "token", Control: "secret", Label: "Token", Inherit: true},
				}
				resourceTypes = []adapterproto.ResourceType{{Type: "alpha"}}
			}
			if mode == "watch" || strings.HasPrefix(mode, "watch_error_") {
				capabilities = append(capabilities, adapterproto.CapabilityWatch)
				if mode == "watch" {
					inputSchema.Fields = append(inputSchema.Fields, adapterproto.Field{Key: "token", Control: "secret", Label: "Token", Required: true})
				}
			}
			if variant == "capabilities" {
				capabilities = append(capabilities, adapterproto.CapabilityMetadata)
			}
			if variant == "schema" {
				configurationSchema.Fields = append(configurationSchema.Fields, adapterproto.Field{Key: "changed", Control: "text", Label: "Changed"})
			}
			if variant == "resources" {
				resourceTypes = append(resourceTypes, adapterproto.ResourceType{Type: "alpha"})
			}
			if strings.HasPrefix(mode, "browse") {
				capabilities = append(capabilities, adapterproto.CapabilityResourceBrowse)
				resourceTypes = []adapterproto.ResourceType{
					{Type: "alpha"},
					{Type: "beta", ParentTypes: []string{"alpha"}},
				}
			}
			if mode == "required_config" || mode == "required_inherited" || mode == "workflow_required" || mode == "workflow_cancel" || mode == "workflow_default" || mode == "workflow_error" {
				configurationSchema.Fields = []adapterproto.Field{{Key: "required_setting", Control: "text", Label: "Required setting", Required: true}}
			}
			if mode == "required_inherited" {
				configurationSchema = adapterproto.Schema{Fields: []adapterproto.Field{}}
				resourceTypes = []adapterproto.ResourceType{
					{Type: "alpha", ConfigurationSchema: adapterproto.Schema{Fields: []adapterproto.Field{{Key: "required_setting", Control: "text", Label: "Required setting", Required: true, Inherit: true}}}},
					{Type: "beta", ParentTypes: []string{"alpha"}, ConfigurationSchema: adapterproto.Schema{Fields: []adapterproto.Field{}}},
				}
			}
			if mode == "workflow_required" || mode == "workflow_cancel" || mode == "workflow_default" || mode == "workflow_error" {
				capabilities = []string{adapterproto.CapabilityResolveWorkflow}
			}
			descriptor := adapterproto.Descriptor{ID: os.Getenv("IR_ADAPTER_ID"), Name: "Test adapter", Version: version, ProtocolVersion: protocolVersion, Capabilities: capabilities, InputSchema: inputSchema, ConfigurationSchema: configurationSchema, ResourceTypes: resourceTypes, MediaTypes: []string{"hls"}}
			if encoded := os.Getenv("IR_ADAPTER_BRANDING"); encoded != "" {
				if icon, decodeErr := base64.StdEncoding.DecodeString(encoded); decodeErr == nil {
					descriptor.Branding = &adapterproto.Branding{Icon: &adapterproto.BrandIcon{MediaType: "image/png", Data: icon}}
				}
			}
			response, _ = adapterproto.Success(request.ID, descriptor)
		case adapterproto.MethodResourceList, adapterproto.MethodResourceSearch:
			marker := os.Getenv("IR_ADAPTER_MARKER")
			if marker != "" {
				file, openErr := os.OpenFile(marker, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
				if openErr != nil {
					return 8
				}
				_, _ = fmt.Fprintf(file, "%s %s\n", request.Method, request.Params)
				_ = file.Close()
			}
			if !strings.HasPrefix(mode, "browse") {
				response = adapterproto.Failure(request.ID, "unsupported_method", "unsupported", nil)
				break
			}
			var params adapterproto.ResourceListParams
			if request.Method == adapterproto.MethodResourceSearch {
				var search adapterproto.ResourceSearchParams
				_ = json.Unmarshal(request.Params, &search)
				params = adapterproto.ResourceListParams{Parent: search.Parent, ResourceType: search.ResourceType, Cursor: search.Cursor, Limit: search.Limit}
			} else {
				_ = json.Unmarshal(request.Params, &params)
			}
			item := adapterproto.Resource{ResourceRef: adapterproto.ResourceRef{Type: "alpha", ID: "root"}, DisplayName: "Root", Attributes: map[string]json.RawMessage{"opaque": json.RawMessage(`{"rank":3}`)}}
			if params.Parent != nil {
				item = adapterproto.Resource{ResourceRef: adapterproto.ResourceRef{Type: "beta", ID: "child", Parent: params.Parent}, DisplayName: "Child", Attributes: map[string]json.RawMessage{"opaque": json.RawMessage(`{"rank":4}`)}}
			}
			if params.ResourceType != "" && params.ResourceType != item.Type {
				item = adapterproto.Resource{}
			}
			items := []adapterproto.Resource{}
			if item.Type != "" {
				items = append(items, item)
			}
			if mode == "browse_bad_parent" && len(items) > 0 {
				items[0].Parent = &adapterproto.ResourceRef{Type: "alpha", ID: "other"}
			}
			if mode == "browse_bad_ref" && len(items) > 0 {
				items[0].Type = "gamma"
			}
			if mode == "browse_duplicate" && len(items) > 0 {
				items = append(items, items[0])
			}
			if mode == "browse_over_limit" {
				items = []adapterproto.Resource{
					{ResourceRef: adapterproto.ResourceRef{Type: "alpha", ID: "root-1"}},
					{ResourceRef: adapterproto.ResourceRef{Type: "alpha", ID: "root-2"}},
				}
			}
			response, _ = adapterproto.Success(request.ID, adapterproto.ResourcePage{Items: items, NextCursor: "next-page"})
		case adapterproto.MethodResolveBegin:
			var params adapterproto.ResolveBeginParams
			_ = json.Unmarshal(request.Params, &params)
			challenge := &adapterproto.WorkflowChallenge{Schema: adapterproto.Schema{Fields: []adapterproto.Field{{Key: "required_setting", Control: "text", Label: "Required setting", Required: true}}}}
			if mode == "workflow_default" {
				challenge = &adapterproto.WorkflowChallenge{Schema: adapterproto.Schema{Fields: []adapterproto.Field{
					{Key: "fallback", Control: "text", Label: "Fallback", Required: true, Default: json.RawMessage(`"default-value"`)},
					{Key: "do_action", Control: "action", Label: "Continue"},
					{Key: "display_status", Control: "status", Label: "Status"},
				}}}
			}
			response, _ = adapterproto.Success(request.ID, adapterproto.ResolveWorkflowResult{State: "configuration_required", WorkflowID: params.WorkflowID, Challenge: challenge})
		case adapterproto.MethodResolveContinue:
			if mode == "workflow_error" {
				var params adapterproto.ResolveContinueParams
				_ = json.Unmarshal(request.Params, &params)
				response, _ = adapterproto.Success(request.ID, adapterproto.ResolveWorkflowResult{State: "error", WorkflowID: params.WorkflowID})
				break
			}
			if mode == "workflow_cancel" {
				marker := os.Getenv("IR_ADAPTER_MARKER")
				if marker != "" {
					_ = os.WriteFile(marker, []byte("continue"), 0600)
				}
				time.Sleep(30 * time.Second)
				return 7
			}
			if mode == "workflow_default" {
				var params adapterproto.ResolveContinueParams
				_ = json.Unmarshal(request.Params, &params)
				if string(params.Answers["fallback"]) != `"default-value"` {
					response = adapterproto.Failure(request.ID, "missing_default", "default was not forwarded", nil)
				} else {
					response, _ = adapterproto.Success(request.ID, adapterproto.ResolveWorkflowResult{State: "resolved", WorkflowID: params.WorkflowID, Media: &adapterproto.MediaSource{Type: "hls", ManifestURL: "https://default.example/live.m3u8"}})
				}
				break
			}
			response = adapterproto.Failure(request.ID, "unsupported_method", "unsupported", nil)
		case adapterproto.MethodResolve:
			if mode == "countresolve" || mode == "required_config" || mode == "required_inherited" {
				marker := os.Getenv("IR_ADAPTER_MARKER")
				file, openErr := os.OpenFile(marker, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
				if openErr != nil {
					return 6
				}
				_, _ = file.WriteString("x")
				_ = file.Close()
			}
			if mode == "crashresolve" {
				return 4
			}
			if mode == "badresolve" {
				_, _ = io.WriteString(os.Stdout, "not-json\n")
				return 4
			}
			if mode == "mismatchresolve" {
				response, _ = adapterproto.Success("wrong-id", map[string]any{})
				_ = adapterproto.WriteResponse(os.Stdout, response)
				return 4
			}
			if mode == "hangresolve" {
				time.Sleep(30 * time.Second)
				return 5
			}
			if mode == "structured_error" {
				const sentinel = "secret-sentinel"
				response = adapterproto.Failure(request.ID, sentinel, sentinel, map[string]string{"echo": sentinel})
			} else {
				var params adapterproto.ResolveParams
				_ = json.Unmarshal(request.Params, &params)
				if mode == "required_config" || mode == "required_inherited" {
					if len(params.Configuration["required_setting"]) == 0 {
						response = adapterproto.Failure(request.ID, "missing_configuration", "required configuration was not forwarded", nil)
						break
					}
				}
				var input struct {
					ManifestURL string `json:"manifest_url"`
				}
				_ = json.Unmarshal(params.Input, &input)
				media := adapterproto.MediaSource{Type: "hls", ManifestURL: input.ManifestURL}
				if mode == "state" {
					if len(params.State) > 0 && params.State[0].Secrets["session"] == "opaque-state-sentinel" {
						media.ManifestURL = "https://state.example/persisted.m3u8"
					}
					result := adapterproto.ResolveResult{Media: media}
					if len(params.State) == 0 || params.State[0].Secrets["session"] == "" {
						result.State = []adapterproto.StateMutation{{Secrets: map[string]string{"session": "opaque-state-sentinel"}}}
					}
					response, _ = adapterproto.Success(request.ID, result)
				} else {
					response, _ = adapterproto.Success(request.ID, media)
				}
			}
		case adapterproto.MethodWatchCheck:
			if strings.HasPrefix(mode, "watch_error_") {
				code := strings.TrimPrefix(mode, "watch_error_")
				response = adapterproto.Failure(request.ID, code, "remote-message-secret-sentinel", map[string]string{"detail": "remote-details-secret-sentinel"})
				break
			}
			if mode != "watch" {
				response = adapterproto.Failure(request.ID, "unsupported_method", "unsupported", nil)
				break
			}
			var params adapterproto.WatchCheckParams
			if json.Unmarshal(request.Params, &params) != nil {
				response = adapterproto.Failure(request.ID, "invalid_params", "invalid watch params", nil)
				break
			}
			var input struct {
				ManifestURL string `json:"manifest_url"`
				Token       string `json:"token"`
			}
			_ = json.Unmarshal(params.Input, &input)
			if input.Token != "split-watch-secret" || params.Secrets["token"] != "" || input.ManifestURL == "" {
				response = adapterproto.Failure(request.ID, "invalid_watch_input", "invalid watch input", nil)
				break
			}
			media := adapterproto.MediaSource{Type: "hls", ManifestURL: input.ManifestURL}
			response, _ = adapterproto.Success(request.ID, adapterproto.WatchCheckResult{State: "live", SessionRef: "opaque-session", Media: &media})
		case adapterproto.MethodRefresh:
			if mode != "refresh" {
				response = adapterproto.Failure(request.ID, "unsupported_method", "unsupported", nil)
				break
			}
			response, _ = adapterproto.Success(request.ID, adapterproto.RefreshResult{Media: adapterproto.MediaSource{Type: "hls", ManifestURL: "https://refresh.example/new.m3u8"}, State: []adapterproto.StateMutation{{Secrets: map[string]string{"session": "refresh-state-sentinel"}}}})
		case adapterproto.MethodMetadata:
			if mode != "metadata" {
				response = adapterproto.Failure(request.ID, "unsupported_method", "unsupported", nil)
				break
			}
			var params adapterproto.MetadataParams
			if json.Unmarshal(request.Params, &params) != nil {
				response = adapterproto.Failure(request.ID, "invalid_metadata_context", "invalid metadata context", nil)
				break
			}
			checks := []bool{params.Resource != nil && params.Resource.ID == "child", params.Configuration["label"] != nil && string(params.Configuration["label"]) == `"stored"`, params.Secrets["token"] == "private-config-secret", params.Current.ManifestURL == "https://current.example/live.m3u8", hasStateSecret(params.State, "session", "private-state-secret")}
			if checks[0] && checks[1] && checks[2] && checks[3] && checks[4] {
				title, description := "fixture title", ""
				response, _ = adapterproto.Success(request.ID, adapterproto.MetadataResult{Metadata: adapterproto.StreamMetadata{Title: &title, Description: &description}, StateMutations: []adapterproto.StateMutation{{Secrets: map[string]string{"session": "metadata-updated-secret"}}}})
			} else {
				title := fmt.Sprintf("context checks %v", checks)
				response, _ = adapterproto.Success(request.ID, adapterproto.MetadataResult{Metadata: adapterproto.StreamMetadata{Title: &title}})
			}
		case adapterproto.MethodShutdown:
			if mode == "hang_shutdown" {
				time.Sleep(30 * time.Second)
				return 0
			}
			response, _ = adapterproto.Success(request.ID, map[string]bool{"stopped": true})
		default:
			response = adapterproto.Failure(request.ID, "unsupported_method", "unsupported", nil)
		}
		if mode == "response_protocol_mismatch" && request.Method == adapterproto.MethodDescribe {
			response.ProtocolVersion = adapterproto.Version + 1
		}
		_ = adapterproto.WriteResponse(os.Stdout, response)
		if mode == "noread-after-describe" && request.Method == adapterproto.MethodDescribe {
			time.Sleep(30 * time.Second)
			return 0
		}
		if request.Method == adapterproto.MethodShutdown || mode == "exitafterdescribe" && request.Method == adapterproto.MethodDescribe || mode == "browse_exitafterlist" && request.Method == adapterproto.MethodResourceList {
			return 0
		}
	}
}

func hasStateSecret(documents []adapterproto.StateDocument, key, value string) bool {
	for _, document := range documents {
		if document.Secrets[key] == value {
			return true
		}
	}
	return false
}

func TestDiscoverExecutableAndIgnoresNonExecutable(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, "integrated-recorder-adapter-ready", "normal", "ready", true)
	writeAdapter(t, dir, "integrated-recorder-adapter-ignored", "normal", "ignored", false)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	list := host.List()
	if len(list) != 1 || list[0].Status.ID != "ready" || list[0].Status.State != "ready" {
		t.Fatalf("adapters = %#v", list)
	}
	got, err := host.Get("ready")
	if err != nil || got.Descriptor == nil || got.Descriptor.Name != "Test adapter" {
		t.Fatalf("Get = %#v, %v", got, err)
	}
}

func TestBrowseResourcesDispatchesListAndSearchWithOpaqueData(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "calls")
	writeAdapterWithMarker(t, dir, "browse", "browse", marker)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	page, err := host.BrowseResources(context.Background(), "browse", nil, "alpha", "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Type != "alpha" || page.Items[0].ID != "root" || string(page.Items[0].Attributes["opaque"]) != `{"rank":3}` || page.NextCursor != "next-page" {
		t.Fatalf("list page = %#v", page)
	}
	parent := &adapterproto.ResourceRef{Type: "alpha", ID: "root"}
	page, err = host.BrowseResources(context.Background(), "browse", parent, "beta", "opaque query", "cursor-in", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Type != "beta" || page.Items[0].Parent == nil || page.Items[0].Parent.ID != "root" {
		t.Fatalf("search page = %#v", page)
	}

	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("adapter calls = %q", data)
	}
	if !strings.HasPrefix(lines[0], adapterproto.MethodResourceList+" ") || !strings.HasPrefix(lines[1], adapterproto.MethodResourceSearch+" ") {
		t.Fatalf("adapter methods = %q", lines)
	}
	var listParams adapterproto.ResourceListParams
	if err = json.Unmarshal([]byte(strings.TrimPrefix(lines[0], adapterproto.MethodResourceList+" ")), &listParams); err != nil {
		t.Fatal(err)
	}
	if listParams.Limit != 20 || listParams.ResourceType != "alpha" {
		t.Fatalf("list params = %#v", listParams)
	}
	var searchParams adapterproto.ResourceSearchParams
	if err = json.Unmarshal([]byte(strings.TrimPrefix(lines[1], adapterproto.MethodResourceSearch+" ")), &searchParams); err != nil {
		t.Fatal(err)
	}
	if searchParams.Limit != 50 || searchParams.Query != "opaque query" || searchParams.Cursor != "cursor-in" || searchParams.Parent == nil || searchParams.Parent.ID != "root" {
		t.Fatalf("search params = %#v", searchParams)
	}
}

func TestBrowseResourcesUnsupportedCapabilityDoesNotInvokeAdapter(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "calls")
	writeAdapterWithMarker(t, dir, "normal", "plain", marker)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err = host.BrowseResources(context.Background(), "plain", nil, "", "", "", 10); !errors.Is(err, ErrUnsupportedResourceBrowse) {
		t.Fatalf("unsupported error = %v", err)
	}
	if _, err = os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("adapter was invoked for unsupported capability; marker stat error = %v", err)
	}
}

func TestBrowseResourcesRejectsInvalidInputAndPages(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		parent   *adapterproto.ResourceRef
		typeName string
		query    string
		cursor   string
		limit    int
	}{
		{name: "undeclared parent", mode: "browse", parent: &adapterproto.ResourceRef{Type: "gamma", ID: "x"}},
		{name: "undeclared requested type", mode: "browse", typeName: "gamma"},
		{name: "long query", mode: "browse", query: strings.Repeat("q", maxResourceQueryBytes+1)},
		{name: "long cursor", mode: "browse", cursor: strings.Repeat("c", maxResourceCursorBytes+1)},
		{name: "negative limit", mode: "browse", limit: -1},
		{name: "outside parent", mode: "browse_bad_parent", parent: &adapterproto.ResourceRef{Type: "alpha", ID: "root"}, typeName: "beta", limit: 2},
		{name: "undeclared returned ref", mode: "browse_bad_ref", limit: 2},
		{name: "duplicate refs", mode: "browse_duplicate", limit: 2},
		{name: "over limit", mode: "browse_over_limit", limit: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			writeAdapter(t, dir, binaryPrefix+"browse", tt.mode, "browse", true)
			host, err := Discover(context.Background(), dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer host.Close()
			_, err = host.BrowseResources(context.Background(), "browse", tt.parent, tt.typeName, tt.query, tt.cursor, tt.limit)
			if err == nil {
				t.Fatal("expected request or page rejection")
			}
		})
	}
}

func TestBrowseResourcesRecoversThroughSupervisedProcessRestart(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "started")
	callLog := filepath.Join(dir, "calls")
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nif [ ! -f " + shellQuote(marker) + " ]; then : > " + shellQuote(marker) + "; IR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=browse_exitafterlist IR_ADAPTER_ID=browse IR_ADAPTER_MARKER=" + shellQuote(callLog) + " exec " + shellQuote(binary) + "; fi\nIR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=browse IR_ADAPTER_ID=browse IR_ADAPTER_MARKER=" + shellQuote(callLog) + " exec " + shellQuote(binary) + "\n"
	if err = os.WriteFile(filepath.Join(dir, binaryPrefix+"browse"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	first, err := host.BrowseResources(context.Background(), "browse", nil, "alpha", "", "", 1)
	if err != nil || len(first.Items) != 1 {
		t.Fatalf("first browse = %#v, %v", first, err)
	}
	var second adapterproto.ResourcePage
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		second, err = host.BrowseResources(context.Background(), "browse", nil, "alpha", "", "", 1)
		if err == nil && len(second.Items) == 1 {
			break
		}
		// The first call after a just-exited child may observe EOF before the
		// supervisor has marked it unusable. The following call must recover
		// through the normal bounded restart path.
		time.Sleep(120 * time.Millisecond)
	}
	if err != nil || len(second.Items) != 1 {
		t.Fatalf("browse after supervised restart = %#v, %v", second, err)
	}
	adapter, err := host.Get("browse")
	if err != nil || adapter.Status.Generation < 2 || adapter.Status.State != "ready" {
		t.Fatalf("restarted adapter = %#v, %v", adapter.Status, err)
	}
}

func TestDuplicateIDRejectedAndFailedProcessIsolated(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, "integrated-recorder-adapter-one", "normal", "duplicate", true)
	writeAdapter(t, dir, "integrated-recorder-adapter-two", "normal", "duplicate", true)
	writeAdapter(t, dir, "integrated-recorder-adapter-bad", "badjson", "bad", true)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err = host.Get("duplicate"); err != nil {
		t.Fatal(err)
	}
	duplicate, _ := host.Get("duplicate")
	if duplicate.Status.State != "rejected" {
		t.Fatalf("duplicate status = %#v", duplicate.Status)
	}
	duplicateCount := 0
	for _, adapter := range host.List() {
		if adapter.Status.ID == "duplicate" {
			duplicateCount++
		}
	}
	if duplicateCount != 1 {
		t.Fatalf("duplicate id appeared %d times in discovery list", duplicateCount)
	}
	bad, err := host.Get("bad")
	if err != nil || bad.Status.State != "failed" && bad.Status.State != "unavailable" {
		t.Fatalf("failed adapter status = %#v, %v", bad, err)
	}
}

func TestDuplicateIDAcrossThreeDirectoriesStaysRejected(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	for i, dir := range dirs {
		writeAdapter(t, dir, fmt.Sprintf("integrated-recorder-adapter-duplicate-%d", i), "normal", "duplicate", true)
	}
	host, err := DiscoverDirs(context.Background(), dirs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	list := host.List()
	if len(list) != 1 || list[0].Status.State != "rejected" || list[0].Descriptor != nil {
		t.Fatalf("duplicate adapter discovery = %#v", list)
	}
	got, err := host.Get("duplicate")
	if err != nil || got.Status.State != "rejected" || got.Descriptor != nil {
		t.Fatalf("duplicate adapter lookup = %#v, %v", got, err)
	}
}

func TestWrongRequestIDAndDescribeTimeoutFailSafely(t *testing.T) {
	for _, tc := range []struct{ name, mode string }{{"mismatch", "mismatch"}, {"timeout", "hang"}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeAdapter(t, dir, "integrated-recorder-adapter-"+tc.name, tc.mode, tc.name, true)
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			host, err := Discover(ctx, dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer host.Close()
			adapter, err := host.Get(tc.name)
			if err != nil || adapter.Status.State != "failed" && adapter.Status.State != "unavailable" {
				t.Fatalf("status = %#v, %v", adapter, err)
			}
		})
	}
}

func TestIncompatibleDescriptorVersionIsRejected(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, "integrated-recorder-adapter-incompatible", "incompatible", "incompatible", true)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	adapter, err := host.Get("incompatible")
	if err != nil || adapter.Status.State != "failed" {
		t.Fatalf("incompatible adapter = %#v, %v", adapter, err)
	}
}

func TestResolveUsesGenericInputAndHidesStructuredAdapterErrorFields(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, "integrated-recorder-adapter-good", "normal", "good", true)
	writeAdapter(t, dir, "integrated-recorder-adapter-errors", "structured_error", "errors", true)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	media, err := host.Resolve(context.Background(), "good", json.RawMessage(`{"manifest_url":"https://stream.example/live.m3u8"}`), nil)
	if err != nil || media.Type != "hls" || media.ManifestURL != "https://stream.example/live.m3u8" {
		t.Fatalf("resolve = %#v, %v", media, err)
	}
	_, err = host.Resolve(context.Background(), "errors", json.RawMessage(`{"manifest_url":"https://stream.example/live.m3u8"}`), nil)
	if err == nil || err.Error() != "adapter returned an error" || strings.Contains(err.Error(), "secret-sentinel") {
		t.Fatalf("adapter error was not replaced with a stable generic error: %v", err)
	}
}

func TestEffectiveConfigurationDoesNotCrossSchemaClassifications(t *testing.T) {
	schema := adapterproto.Schema{Fields: []adapterproto.Field{
		{Key: "formerly_secret", Control: "text", Label: "Formerly secret"},
		{Key: "formerly_text", Control: "secret", Label: "Formerly text"},
		{Key: "current_secret", Control: "secret", Label: "Current secret"},
		{Key: "current_text", Control: "text", Label: "Current text"},
	}}
	effective := pluginconfig.EffectiveDocument{
		EffectiveValues: map[string]json.RawMessage{
			"formerly_text": json.RawMessage(`"ordinary-value-must-not-become-secret"`),
			"current_text":  json.RawMessage(`"ordinary-value"`),
		},
		EffectiveSecrets: map[string]string{
			"formerly_secret": "old-secret-must-not-become-config",
			"current_secret":  "current-secret",
		},
	}
	values, secrets, err := effectiveConfiguration(schema, effective, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := values["formerly_secret"]; exists {
		t.Fatal("previously secret data leaked into ordinary configuration")
	}
	if _, exists := secrets["formerly_secret"]; exists {
		t.Fatal("previously secret field remained an adapter secret after reclassification")
	}
	if _, exists := values["formerly_text"]; exists {
		t.Fatal("previously ordinary data leaked into secret configuration")
	}
	if _, exists := secrets["formerly_text"]; exists {
		t.Fatal("previously ordinary field was sent in adapter secrets")
	}
	if string(values["current_text"]) != `"ordinary-value"` || secrets["current_secret"] != "current-secret" {
		t.Fatalf("same-category values were not forwarded: values=%s secrets=%#v", values["current_text"], secrets)
	}
}

func TestInputSchemaRejectsBeforeAdapterResolve(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "resolve-called")
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nIR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=countresolve IR_ADAPTER_ID=counted IR_ADAPTER_MARKER=" + shellQuote(marker) + " exec " + shellQuote(binary) + "\n"
	if err = os.WriteFile(filepath.Join(dir, binaryPrefix+"counted"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err = host.Resolve(context.Background(), "counted", json.RawMessage(`{"manifest_url":"https://example.test/live.m3u8","unknown":"private-value"}`), nil); err == nil {
		t.Fatal("unknown input field was accepted")
	}
	if _, err = os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("adapter resolve was called for invalid input: stat error=%v", err)
	}
	if _, err = host.Resolve(context.Background(), "counted", json.RawMessage(`{"manifest_url":"https://example.test/live.m3u8"}`), nil); err != nil {
		t.Fatalf("valid input rejected: %v", err)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "x" {
		t.Fatalf("adapter resolve calls=%q, err=%v", data, err)
	}
}

func TestProcessExitUpdatesReportedStatus(t *testing.T) {
	dir := t.TempDir()
	writeExitingAdapter(t, dir, "integrated-recorder-adapter-exit")
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		item, _ := host.Get("exit")
		if item.Status.State == "unavailable" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	item, _ := host.Get("exit")
	t.Fatalf("process exit state = %q", item.Status.State)
}

func TestProcessDrainsResponseBeforeReapingAdapter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "exit-after-response")
	script := "#!/bin/sh\nIFS= read -r request || exit 1\nprintf '%s\\n' '{\"protocol_version\":1,\"id\":\"1\",\"result\":{\"ok\":true}}'\nexit 0\n"
	if err := os.WriteFile(path, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		p, err := startProcess(path)
		if err != nil {
			t.Fatal(err)
		}
		result, err := p.call(context.Background(), "describe", map[string]any{})
		if err != nil || string(result) != `{"ok":true}` {
			p.kill()
			t.Fatalf("response lost on immediate process exit (iteration %d): %s, %v", i, result, err)
		}
		select {
		case <-p.done:
		case <-time.After(time.Second):
			p.kill()
			t.Fatal("adapter process was not reaped")
		}
	}
}

func TestAdapterOwnedSecretStateSurvivesProcessRestart(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, binaryPrefix+"state", "state", "state", true)
	configs, secrets, state, err := pluginconfig.NewTypedFileStoresAndState(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	configService, err := pluginconfig.NewService(configs, secrets)
	if err != nil {
		t.Fatal(err)
	}
	host, err := DiscoverWithState(context.Background(), dir, configService, state)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	input := json.RawMessage(`{"manifest_url":"https://source.example/live.m3u8"}`)
	first, err := host.Resolve(context.Background(), "state", input, nil)
	if err != nil || first.ManifestURL != "https://source.example/live.m3u8" {
		t.Fatalf("initial resolve = %#v, %v", first, err)
	}
	entry, err := host.entryFor("state")
	if err != nil {
		t.Fatal(err)
	}
	entry.stateMu.Lock()
	entry.process.kill()
	entry.stateMu.Unlock()
	second, err := host.Resolve(context.Background(), "state", input, nil)
	if err != nil || second.ManifestURL != "https://state.example/persisted.m3u8" {
		t.Fatalf("resolve after adapter restart = %#v, %v", second, err)
	}
	doc, err := state.Get(pluginconfig.Scope{PluginID: "state"})
	if err != nil || doc.Secrets["session"] != "opaque-state-sentinel" {
		t.Fatalf("opaque state secret was not persisted: %#v, %v", doc, err)
	}
}

func TestPrepareRefreshDefersStateMutationUntilCommit(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, binaryPrefix+"refresh", "refresh", "refresh", true)
	configs, secrets, state, err := pluginconfig.NewTypedFileStoresAndState(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	configService, err := pluginconfig.NewService(configs, secrets)
	if err != nil {
		t.Fatal(err)
	}
	host, err := DiscoverWithState(context.Background(), dir, configService, state)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	current := adapterproto.MediaSource{Type: "hls", ManifestURL: "https://old.example/live.m3u8", RefreshPolicy: &adapterproto.RefreshPolicy{OnHTTPStatus: []int{451}}}
	media, commit, err := host.PrepareRefresh(context.Background(), "refresh", nil, current)
	if err != nil || media.ManifestURL != "https://refresh.example/new.m3u8" || commit == nil {
		t.Fatalf("prepared refresh = %#v, %v", media, err)
	}
	doc, err := state.Get(pluginconfig.Scope{PluginID: "refresh"})
	if err != nil || len(doc.Secrets) != 0 {
		t.Fatalf("state mutation committed before caller validation: %#v, %v", doc, err)
	}
	if !ShouldRefreshForStatus(current, 451) || ShouldRefreshForStatus(current, 403) {
		t.Fatal("refresh trigger did not follow the adapter-declared status list")
	}
	if err = commit(); err != nil {
		t.Fatal(err)
	}
	doc, err = state.Get(pluginconfig.Scope{PluginID: "refresh"})
	if err != nil || doc.Secrets["session"] != "refresh-state-sentinel" {
		t.Fatalf("staged refresh state was not committed: %#v, %v", doc, err)
	}
}

func TestWorkflowCleanupLimitsAndCumulativeTransitionBound(t *testing.T) {
	tracker := interaction.NewTracker()
	prompt := adapterproto.InteractionMessage{Type: "prompt", InteractionID: "old-interaction"}
	if _, err := tracker.Apply(prompt); err != nil {
		t.Fatal(err)
	}
	host := &Host{interactions: tracker, workflows: map[string]workflowSession{
		"cancel-me": {adapterID: "opaque", workflowID: "cancel-me", updatedAt: time.Now(), progress: WorkflowProgress{Challenge: &adapterproto.WorkflowChallenge{Prompt: &prompt}}},
	}}
	if err := host.CancelWorkflow("cancel-me"); err != nil {
		t.Fatal(err)
	}
	if _, err := tracker.Get(prompt.InteractionID); err == nil {
		t.Fatal("cancelled workflow left interaction state behind")
	}
	release, err := host.reserveWorkflow()
	if err != nil {
		t.Fatal("available workflow slot rejected", err)
	}
	// Release the reservation before exercising the full active limit.
	release()
	// Expiration is lazy and removes its associated interaction state.
	if _, err := tracker.Apply(adapterproto.InteractionMessage{Type: "status", InteractionID: "expired-interaction"}); err != nil {
		t.Fatal(err)
	}
	expiredPrompt := adapterproto.InteractionMessage{Type: "prompt", InteractionID: "expired-interaction"}
	old := time.Now().Add(-workflowTTL - time.Second)
	host.mu.Lock()
	host.workflows["expired"] = workflowSession{adapterID: "opaque", workflowID: "expired", updatedAt: old, progress: WorkflowProgress{Challenge: &adapterproto.WorkflowChallenge{Prompt: &expiredPrompt}}}
	host.mu.Unlock()
	if _, err := host.Workflow("expired"); err == nil {
		t.Fatal("expired workflow remained available")
	}
	if _, err := tracker.Get("expired-interaction"); err == nil {
		t.Fatal("expired workflow left interaction state behind")
	}

	for i := 0; i < maxActiveWorkflows; i++ {
		host.workflows[fmt.Sprintf("active-%d", i)] = workflowSession{adapterID: "opaque", workflowID: fmt.Sprintf("active-%d", i), updatedAt: time.Now()}
	}
	if _, err := host.reserveWorkflow(); err == nil {
		t.Fatal("active workflow bound was not enforced")
	}
	challenge := &adapterproto.WorkflowChallenge{Schema: adapterproto.Schema{Fields: []adapterproto.Field{{Key: "answer", Control: "text", Label: "Answer"}}}}
	descriptor := adapterproto.Descriptor{ID: "opaque", Name: "Opaque", Version: "1", ProtocolVersion: adapterproto.Version, Capabilities: []string{adapterproto.CapabilityResolveWorkflow}, MediaTypes: []string{"hls"}}
	_, err = host.advanceWorkflow(context.Background(), descriptor, workflowSession{adapterID: "opaque", workflowID: "budget", transitions: maxWorkflowTransitions}, adapterproto.ResolveWorkflowResult{State: "configuration_required", WorkflowID: "budget", Challenge: challenge})
	if err == nil || !strings.Contains(err.Error(), "transition limit") {
		t.Fatalf("cumulative transition bound error = %v", err)
	}
}

func TestWorkflowReservationBackpressuresWhenLifecycleQueueIsFull(t *testing.T) {
	host := &Host{
		lifecycleQueue: make([]WorkflowLifecycleEvent, maxPendingWorkflowLifecycleEvents),
	}
	if _, err := host.reserveWorkflow(); err == nil {
		t.Fatal("workflow reservation succeeded with a full lifecycle queue")
	}
}

func TestCancelWorkflowCancelsInFlightAdapterContinuation(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "continuing")
	t.Setenv("IR_ADAPTER_MARKER", marker)
	writeAdapter(t, dir, "integrated-recorder-adapter-workflow-cancel", "workflow_cancel", "workflow-cancel", true)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	progress, err := host.BeginResolution(context.Background(), "workflow-cancel", json.RawMessage(`{"manifest_url":"https://media.example/live.m3u8"}`), nil)
	if err != nil || progress.State != "configuration_required" {
		t.Fatalf("begin workflow = %#v, %v", progress, err)
	}
	continued := make(chan error, 1)
	go func() {
		_, continueErr := host.ContinueResolutionFields(context.Background(), progress.WorkflowID, map[string]json.RawMessage{"required_setting": json.RawMessage(`"answer"`)}, nil, nil)
		continued <- continueErr
	}()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("adapter did not enter continuation")
	}
	if err = host.CancelWorkflow(progress.WorkflowID); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-continued:
		if err == nil {
			t.Fatal("canceled continuation succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("workflow cancellation did not stop the in-flight adapter operation")
	}
	if _, err = host.Workflow(progress.WorkflowID); err == nil {
		t.Fatal("canceled workflow remained active")
	}
}

func TestWorkflowExpiresWhenAdapterGenerationChanges(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, binaryPrefix+"generation", "normal", "generation", true)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	entry, err := host.entryFor("generation")
	if err != nil {
		t.Fatal(err)
	}
	entry.stateMu.Lock()
	entry.process.kill()
	entry.stateMu.Unlock()
	session := workflowSession{adapterID: "generation", workflowID: "workflow", generation: 1, createdAt: time.Now(), updatedAt: time.Now(), progress: WorkflowProgress{WorkflowID: "workflow", AdapterID: "generation", State: "configuration_required", Challenge: &adapterproto.WorkflowChallenge{Schema: adapterproto.Schema{Fields: []adapterproto.Field{{Key: "answer", Control: "text", Label: "Answer"}}}}}}
	host.mu.Lock()
	host.workflows[session.workflowID] = session
	host.mu.Unlock()
	err = host.ensureWorkflowGeneration(context.Background(), session.workflowID, session)
	if err == nil || err.Error() != "workflow expired because adapter process restarted" {
		t.Fatalf("generation mismatch error = %v", err)
	}
	if _, err := host.Workflow(session.workflowID); err == nil {
		t.Fatal("stale workflow survived process restart")
	}
	events := host.PendingWorkflowLifecycleEvents()
	if len(events) != 1 || events[0].WorkflowID != session.workflowID || events[0].State != "expired" {
		t.Fatalf("generation mismatch lifecycle events = %#v", events)
	}
}

func TestChallengeDefaultsAreForwardedOnContinuation(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, binaryPrefix+"workflow-default", "workflow_default", "workflow-default", true)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	progress, err := host.BeginResolution(context.Background(), "workflow-default", json.RawMessage(`{"manifest_url":"https://media.example/live.m3u8"}`), nil)
	if err != nil || progress.State != "configuration_required" {
		t.Fatalf("begin workflow = %#v, %v", progress, err)
	}
	continued, err := host.ContinueResolutionFields(context.Background(), progress.WorkflowID, nil, nil, nil)
	if err != nil || continued.State != "resolved" || continued.Media == nil || continued.Media.ManifestURL != "https://default.example/live.m3u8" {
		t.Fatalf("continuation did not receive declared default: %#v, %v", continued, err)
	}
}

func TestLegacyPersistenceSelectionExcludesDisplayOnlyAndForbiddenFields(t *testing.T) {
	challenge := &adapterproto.WorkflowChallenge{Persistable: true, Schema: adapterproto.Schema{Fields: []adapterproto.Field{
		{Key: "setting", Control: "text", Label: "Setting"},
		{Key: "action", Control: "action", Label: "Action"},
		{Key: "status", Control: "status", Label: "Status"},
		{Key: "temporary", Control: "text", Label: "Temporary", Persistence: &adapterproto.FieldPersistence{Mode: adapterproto.PersistenceForbidden, Target: adapterproto.PersistenceTarget{Scope: adapterproto.PersistenceCurrent}}},
	}}}
	got := legacyPersistableFields(challenge)
	if len(got) != 1 || got[0] != "setting" {
		t.Fatalf("legacy persisted fields = %#v", got)
	}
}

func TestHostCloseShutsDownAdapterProcessesConcurrentlyAndReapsChildren(t *testing.T) {
	dir := t.TempDir()
	for _, id := range []string{"close-one", "close-two", "close-three"} {
		writeAdapter(t, dir, binaryPrefix+id, "hang_shutdown", id, true)
	}
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	children := []*process{}
	for _, id := range []string{"close-one", "close-two", "close-three"} {
		entry, entryErr := host.entryFor(id)
		if entryErr != nil {
			t.Fatal(entryErr)
		}
		entry.stateMu.Lock()
		children = append(children, entry.process)
		entry.stateMu.Unlock()
	}
	started := time.Now()
	host.Close()
	if elapsed := time.Since(started); elapsed >= 5*time.Second {
		t.Fatalf("parallel adapter close took %s, appears serialized", elapsed)
	}
	for index, child := range children {
		select {
		case <-child.done:
		case <-time.After(time.Second):
			t.Fatalf("adapter child %d was not reaped", index)
		}
	}
	if _, err := host.call(context.Background(), "close-one", adapterproto.MethodGetStatus, map[string]any{}); err == nil {
		t.Fatal("closed host accepted another adapter operation")
	}
}

func TestChallengePersistenceTargetsPluginParentCurrentAndRejectsDynamicField(t *testing.T) {
	root := t.TempDir()
	configStore, secretStore, err := pluginconfig.NewTypedFileStores(root)
	if err != nil {
		t.Fatal(err)
	}
	configs, err := pluginconfig.NewService(configStore, secretStore)
	if err != nil {
		t.Fatal(err)
	}
	alpha := &adapterproto.ResourceRef{Type: "alpha", ID: "parent"}
	beta := &adapterproto.ResourceRef{Type: "beta", ID: "child", Parent: alpha}
	descriptor := adapterproto.Descriptor{
		ID: "opaque", Name: "Opaque", Version: "1", ProtocolVersion: adapterproto.Version,
		Capabilities: []string{adapterproto.CapabilityResolveWorkflow}, InputSchema: adapterproto.Schema{},
		ConfigurationSchema: adapterproto.Schema{Fields: []adapterproto.Field{{Key: "plugin_secret", Control: "secret", Label: "Plugin", Inherit: true}}},
		ResourceTypes: []adapterproto.ResourceType{
			{Type: "alpha", ConfigurationSchema: adapterproto.Schema{Fields: []adapterproto.Field{{Key: "parent_secret", Control: "secret", Label: "Parent", Inherit: true}}}},
			{Type: "beta", ParentTypes: []string{"alpha"}, ConfigurationSchema: adapterproto.Schema{Fields: []adapterproto.Field{{Key: "child_secret", Control: "secret", Label: "Child"}}}},
		}, MediaTypes: []string{"hls"},
	}
	if err := descriptor.Validate(); err != nil {
		t.Fatal(err)
	}
	host := &Host{entries: map[string]*entry{"opaque": {descriptor: &descriptor, status: Status{ID: "opaque", State: "ready"}}}, configs: configs, interactions: interaction.NewTracker(), workflows: map[string]workflowSession{}}
	defer host.Close()
	challenge := &adapterproto.WorkflowChallenge{Schema: adapterproto.Schema{Fields: []adapterproto.Field{
		{Key: "plugin_secret", Control: "secret", Label: "Plugin", Required: true, Persistence: &adapterproto.FieldPersistence{Mode: adapterproto.PersistenceRequired, Target: adapterproto.PersistenceTarget{Scope: adapterproto.PersistencePlugin}}},
		{Key: "parent_secret", Control: "secret", Label: "Parent", Required: true, Persistence: &adapterproto.FieldPersistence{Mode: adapterproto.PersistenceRequired, Target: adapterproto.PersistenceTarget{Scope: adapterproto.PersistenceResource, Resource: alpha}}},
		{Key: "child_secret", Control: "secret", Label: "Child", Required: true, Persistence: &adapterproto.FieldPersistence{Mode: adapterproto.PersistenceOptional, Target: adapterproto.PersistenceTarget{Scope: adapterproto.PersistenceCurrent}}},
		{Key: "ephemeral", Control: "secret", Label: "Ephemeral", Required: true, Persistence: &adapterproto.FieldPersistence{Mode: adapterproto.PersistenceForbidden, Target: adapterproto.PersistenceTarget{Scope: adapterproto.PersistenceCurrent}}},
	}}}
	answers := map[string]string{"plugin_secret": "p", "parent_secret": "a", "child_secret": "b", "ephemeral": "e"}
	if _, err := host.persistChallengeAnswers(workflowSession{adapterID: "opaque", resource: beta}, challenge, nil, answers, []string{"child_secret"}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		scope pluginconfig.Scope
		key   string
		want  string
	}{{pluginconfig.Scope{PluginID: "opaque"}, "plugin_secret", "p"}, {pluginconfig.Scope{PluginID: "opaque", Resource: alpha}, "parent_secret", "a"}, {pluginconfig.Scope{PluginID: "opaque", Resource: beta}, "child_secret", "b"}} {
		doc, err := configs.Get(tc.scope)
		if err != nil || doc.Secrets[tc.key] != tc.want {
			t.Fatalf("persistent target %#v field %q=%q, err=%v", tc.scope, tc.key, doc.Secrets[tc.key], err)
		}
	}
	childDoc, err := configs.Get(pluginconfig.Scope{PluginID: "opaque", Resource: beta})
	if err != nil || childDoc.Secrets["ephemeral"] != "" {
		t.Fatalf("forbidden answer was persisted: %#v, %v", childDoc.Secrets, err)
	}
	dynamic := &adapterproto.WorkflowChallenge{Schema: adapterproto.Schema{Fields: []adapterproto.Field{{Key: "runtime_only", Control: "secret", Label: "Runtime", Required: true, Persistence: &adapterproto.FieldPersistence{Mode: adapterproto.PersistenceRequired, Target: adapterproto.PersistenceTarget{Scope: adapterproto.PersistenceCurrent}}}}}}
	if _, err := host.persistChallengeAnswers(workflowSession{adapterID: "opaque", resource: beta}, dynamic, nil, map[string]string{"runtime_only": "not-stored"}, nil); err == nil {
		t.Fatal("undeclared dynamic challenge field was persisted")
	}
}

func TestAdapterLazilyRestartsAfterUnusableCalls(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mode   string
		cancel bool
	}{{"crashresolve", "crashresolve", false}, {"badresolve", "badresolve", false}, {"mismatchresolve", "mismatchresolve", false}, {"timeout", "hangresolve", false}, {"cancel-after-write", "hangresolve", true}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeRestartingAdapter(t, dir, "restart", tc.mode, "restart", "normal")
			host, err := Discover(context.Background(), dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer host.Close()
			ctx := context.Background()
			if tc.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				time.AfterFunc(100*time.Millisecond, cancel)
			} else if tc.name == "timeout" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
			}
			_, firstErr := host.Resolve(ctx, "restart", json.RawMessage(`{"manifest_url":"https://example.test/live.m3u8"}`), nil)
			if firstErr == nil {
				t.Fatal("first unusable operation unexpectedly succeeded")
			}
			time.Sleep(120 * time.Millisecond)
			media, err := host.Resolve(context.Background(), "restart", json.RawMessage(`{"manifest_url":"https://example.test/live.m3u8"}`), nil)
			if err != nil || media.ManifestURL != "https://example.test/live.m3u8" {
				t.Fatalf("operation after restart = %#v, %v", media, err)
			}
			adapter, _ := host.Get("restart")
			if adapter.Status.Generation < 2 || adapter.Status.State != "ready" {
				t.Fatalf("restart status = %#v", adapter.Status)
			}
		})
	}
}

func TestCanceledBeforeRequestWriteKeepsAdapterUsable(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, "integrated-recorder-adapter-cancel-safe", "normal", "cancel-safe", true)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = host.Resolve(ctx, "cancel-safe", json.RawMessage(`{"manifest_url":"https://example.test/live.m3u8"}`), nil); err == nil {
		t.Fatal("canceled operation succeeded")
	}
	media, err := host.Resolve(context.Background(), "cancel-safe", json.RawMessage(`{"manifest_url":"https://example.test/live.m3u8"}`), nil)
	if err != nil || media.ManifestURL == "" {
		t.Fatalf("healthy process was lost after pre-cancel: %#v %v", media, err)
	}
}

func TestDescriptorFingerprintExcludesBrandingAndRetainsLegacyHash(t *testing.T) {
	base := adapterproto.Descriptor{ID: "adapter", Name: "Adapter", Version: "1", ProtocolVersion: adapterproto.Version, Capabilities: []string{adapterproto.CapabilityResolve}, MediaTypes: []string{"hls"}}
	const legacyHash = "6e7cb88d988a89940e243877949f833436d3b5a4fc23ee80e505f460e2be5564"
	if got := descriptorFingerprint(base); got != legacyHash {
		t.Fatalf("unbranded fingerprint changed: got %s want legacy %s", got, legacyHash)
	}
	branded := base
	branded.Branding = &adapterproto.Branding{Icon: &adapterproto.BrandIcon{MediaType: "image/png", Data: hostTestPNG(t, color.NRGBA{R: 1, A: 255})}}
	if got := descriptorFingerprint(branded); got != legacyHash {
		t.Fatalf("branding changed semantic fingerprint: got %s want %s", got, legacyHash)
	}

	semanticChanges := map[string]func(*adapterproto.Descriptor){
		"id":       func(d *adapterproto.Descriptor) { d.ID = "other-adapter" },
		"name":     func(d *adapterproto.Descriptor) { d.Name = "Other" },
		"version":  func(d *adapterproto.Descriptor) { d.Version = "2" },
		"protocol": func(d *adapterproto.Descriptor) { d.ProtocolVersion++ },
		"schema": func(d *adapterproto.Descriptor) {
			d.ConfigurationSchema.Fields = []adapterproto.Field{{Key: "value", Control: "text", Label: "Value"}}
		},
		"capability": func(d *adapterproto.Descriptor) {
			d.Capabilities = append(d.Capabilities, adapterproto.CapabilityMetadata)
		},
		"resource":   func(d *adapterproto.Descriptor) { d.ResourceTypes = []adapterproto.ResourceType{{Type: "opaque"}} },
		"media type": func(d *adapterproto.Descriptor) { d.MediaTypes = append(d.MediaTypes, "dash") },
	}
	for name, mutate := range semanticChanges {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			if descriptorFingerprint(changed) == legacyHash {
				t.Fatal("semantic descriptor change did not change fingerprint")
			}
		})
	}
}

func TestManualRestartAcceptsBrandingOnlyChangeAndAdoptsNewIcon(t *testing.T) {
	dir := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	firstIcon := hostTestPNG(t, color.NRGBA{R: 10, G: 20, B: 30, A: 255})
	secondIcon := hostTestPNG(t, color.NRGBA{R: 200, G: 150, B: 100, A: 255})
	marker := filepath.Join(dir, "started")
	script := "#!/bin/sh\nif [ ! -f " + shellQuote(marker) + " ]; then : > " + shellQuote(marker) + "; IR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=normal IR_ADAPTER_ID=brand-restart IR_ADAPTER_BRANDING=" + shellQuote(base64.StdEncoding.EncodeToString(firstIcon)) + " exec " + shellQuote(binary) + "; fi\nIR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=normal IR_ADAPTER_ID=brand-restart IR_ADAPTER_BRANDING=" + shellQuote(base64.StdEncoding.EncodeToString(secondIcon)) + " exec " + shellQuote(binary) + "\n"
	if err := os.WriteFile(filepath.Join(dir, binaryPrefix+"brand-restart"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	before, err := host.Get("brand-restart")
	if err != nil || before.Descriptor == nil || before.Descriptor.Branding == nil || !bytes.Equal(before.Descriptor.Branding.Icon.Data, firstIcon) {
		t.Fatalf("initial descriptor=%#v err=%v", before.Descriptor, err)
	}
	if _, err := host.Restart(context.Background(), "brand-restart"); err != nil {
		t.Fatalf("branding-only restart rejected: %v", err)
	}
	after, err := host.Get("brand-restart")
	if err != nil || after.Descriptor == nil || after.Descriptor.Branding == nil || after.Descriptor.Branding.Icon == nil || !bytes.Equal(after.Descriptor.Branding.Icon.Data, secondIcon) {
		t.Fatalf("restarted descriptor did not adopt current branding: %#v err=%v", after.Descriptor, err)
	}
	listed := host.List()
	if len(listed) != 1 || listed[0].Descriptor == nil || listed[0].Descriptor.Branding == nil || listed[0].Descriptor.Branding.Icon == nil || !bytes.Equal(listed[0].Descriptor.Branding.Icon.Data, secondIcon) {
		t.Fatalf("list did not return current branding: %#v", listed)
	}
	// Returned branding is a copy; callers cannot mutate the host's live view.
	after.Descriptor.Branding.Icon.Data[0] ^= 0xff
	current, _ := host.Get("brand-restart")
	if current.Descriptor == nil || current.Descriptor.Branding == nil || current.Descriptor.Branding.Icon == nil || !bytes.Equal(current.Descriptor.Branding.Icon.Data, secondIcon) {
		t.Fatal("caller mutation changed the host descriptor")
	}
}

func hostTestPNG(t *testing.T, pixel color.NRGBA) []byte {
	t.Helper()
	img := image.NewNRGBA(image.Rect(0, 0, 1, 1))
	img.SetNRGBA(0, 0, pixel)
	var output bytes.Buffer
	if err := png.Encode(&output, img); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func TestCanceledRequestUnblocksLargeStdinWrite(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, "integrated-recorder-adapter-no-reader", "noread-after-describe", "no-reader", true)
	p, err := startProcess(filepath.Join(dir, "integrated-recorder-adapter-no-reader"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.call(context.Background(), adapterproto.MethodDescribe, map[string]any{})
	if err != nil {
		p.kill()
		t.Fatalf("describe = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = p.call(ctx, "large", map[string]string{"payload": strings.Repeat("x", 6<<20)})
	if err == nil || time.Since(started) > 2*time.Second {
		p.kill()
		t.Fatalf("large blocked write cancellation = %v after %s", err, time.Since(started))
	}
	select {
	case <-p.done:
	case <-time.After(2 * time.Second):
		p.kill()
		t.Fatal("canceled blocked writer did not terminate its adapter")
	}
}

func TestRestartRejectsChangedDescriptorAndBacksOff(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "spawned")
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nif [ ! -f " + shellQuote(marker) + " ]; then MODE=crashresolve; ID=stable; else MODE=normal; ID=changed; fi\nprintf x >> " + shellQuote(marker) + "\nIR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=$MODE IR_ADAPTER_ID=$ID exec " + shellQuote(binary) + "\n"
	if err = os.WriteFile(filepath.Join(dir, "integrated-recorder-adapter-stable"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	input := json.RawMessage(`{"manifest_url":"https://example.test/live.m3u8"}`)
	_, _ = host.Resolve(context.Background(), "stable", input, nil)
	time.Sleep(120 * time.Millisecond)
	if _, err = host.Resolve(context.Background(), "stable", input, nil); err == nil {
		t.Fatal("changed descriptor was accepted")
	}
	if _, err = host.Resolve(context.Background(), "stable", input, nil); err == nil {
		t.Fatal("rejected process was retried")
	}
	countData, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if string(countData) != "xx" {
		t.Fatalf("restart storm or missing replacement spawn; process count=%q", countData)
	}
	adapter, _ := host.Get("stable")
	if adapter.Status.State != "rejected" {
		t.Fatalf("rejected adapter state = %#v", adapter.Status)
	}
}

func TestRestartRejectsIncompatibleProtocolVersion(t *testing.T) {
	dir := t.TempDir()
	writeRestartingAdapter(t, dir, "stable", "crashresolve", "stable", "incompatible")
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	input := json.RawMessage(`{"manifest_url":"https://example.test/live.m3u8"}`)
	if _, err = host.Resolve(context.Background(), "stable", input, nil); err == nil {
		t.Fatal("crashing first process unexpectedly resolved")
	}
	time.Sleep(120 * time.Millisecond)
	if _, err = host.Resolve(context.Background(), "stable", input, nil); err == nil {
		t.Fatal("incompatible replacement protocol was accepted")
	}
	adapter, _ := host.Get("stable")
	if adapter.Status.State != "rejected" {
		t.Fatalf("incompatible replacement status = %#v", adapter.Status)
	}
	if _, err = host.Resolve(context.Background(), "stable", input, nil); err == nil {
		t.Fatal("rejected adapter unexpectedly retried")
	}
}

func TestRestartRejectsResponseEnvelopeProtocolMismatchWithoutRetry(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "spawned")
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nif [ ! -f " + shellQuote(marker) + " ]; then printf x >> " + shellQuote(marker) + "; IR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=crashresolve IR_ADAPTER_ID=stable exec " + shellQuote(binary) + "; fi\nprintf x >> " + shellQuote(marker) + "\nIR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=response_protocol_mismatch IR_ADAPTER_ID=stable exec " + shellQuote(binary) + "\n"
	if err = os.WriteFile(filepath.Join(dir, binaryPrefix+"stable"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	input := json.RawMessage(`{"manifest_url":"https://example.test/live.m3u8"}`)
	if _, err = host.Resolve(context.Background(), "stable", input, nil); err == nil {
		t.Fatal("initial process unexpectedly resolved")
	}
	time.Sleep(120 * time.Millisecond)
	if _, err = host.Resolve(context.Background(), "stable", input, nil); err == nil {
		t.Fatal("replacement with mismatched response protocol was accepted")
	}
	if _, err = host.Resolve(context.Background(), "stable", input, nil); err == nil {
		t.Fatal("rejected adapter was retried")
	}
	count, err := os.ReadFile(marker)
	if err != nil || string(count) != "xx" {
		t.Fatalf("mismatched adapter was respawned: count=%q err=%v", count, err)
	}
	adapter, _ := host.Get("stable")
	if adapter.Status.State != "rejected" {
		t.Fatalf("mismatched adapter status = %#v", adapter.Status)
	}
}

func TestRestartRejectsDescriptorFingerprintChanges(t *testing.T) {
	for _, variant := range []string{"version", "schema", "capabilities", "resources"} {
		t.Run(variant, func(t *testing.T) {
			dir := t.TempDir()
			marker := filepath.Join(dir, "spawned")
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			later := "IR_ADAPTER_VARIANT=" + shellQuote(variant)
			if variant == "version" {
				later = "IR_ADAPTER_VERSION=2"
			}
			script := "#!/bin/sh\nif [ ! -f " + shellQuote(marker) + " ]; then : > " + shellQuote(marker) + "; IR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=crashresolve IR_ADAPTER_ID=stable exec " + shellQuote(binary) + "; fi\nprintf x >> " + shellQuote(marker) + "\nIR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=normal IR_ADAPTER_ID=stable " + later + " exec " + shellQuote(binary) + "\n"
			if err = os.WriteFile(filepath.Join(dir, binaryPrefix+"stable"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			host, err := Discover(context.Background(), dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer host.Close()
			input := json.RawMessage(`{"manifest_url":"https://example.test/live.m3u8"}`)
			if _, err = host.Resolve(context.Background(), "stable", input, nil); err == nil {
				t.Fatal("first process did not crash")
			}
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				_, _ = host.Resolve(context.Background(), "stable", input, nil)
				adapter, _ := host.Get("stable")
				if adapter.Status.State == "rejected" {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			count, err := os.ReadFile(marker)
			if err != nil || string(count) != "x" {
				adapter, _ := host.Get("stable")
				t.Fatalf("replacement was not isolated after one spawn: %q %v status=%#v", count, err, adapter.Status)
			}
			adapter, _ := host.Get("stable")
			if adapter.Status.State != "rejected" {
				t.Fatalf("mismatch state=%#v", adapter.Status)
			}
		})
	}
}

func TestShortLivedAdapterRestartsUseGrowingBackoff(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "spawned")
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nprintf x >> " + shellQuote(marker) + "\nIR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=crashresolve IR_ADAPTER_ID=unstable exec " + shellQuote(binary) + "\n"
	if err = os.WriteFile(filepath.Join(dir, binaryPrefix+"unstable"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	input := json.RawMessage(`{"manifest_url":"https://example.test/live.m3u8"}`)
	if _, err = host.Resolve(context.Background(), "unstable", input, nil); err == nil {
		t.Fatal("short-lived adapter unexpectedly resolved")
	}
	e, err := host.entryFor("unstable")
	if err != nil {
		t.Fatal(err)
	}
	e.stateMu.Lock()
	e.nextRestart = time.Now().Add(time.Hour)
	e.stateMu.Unlock()
	if _, err = host.Resolve(context.Background(), "unstable", input, nil); err == nil {
		t.Fatal("adapter restarted during backoff")
	}
	if data, readErr := os.ReadFile(marker); readErr != nil || string(data) != "x" {
		t.Fatalf("backoff did not suppress a rapid spawn: count=%q err=%v", data, readErr)
	}

	for attempt := 1; attempt <= 3; attempt++ {
		e.stateMu.Lock()
		e.nextRestart = time.Time{} // deterministic clock advance past the recorded backoff
		e.stateMu.Unlock()
		if _, err = host.Resolve(context.Background(), "unstable", input, nil); err == nil {
			t.Fatalf("short-lived generation %d unexpectedly resolved", attempt)
		}
		e.stateMu.Lock()
		gotAttempt := e.status.RestartAttempts
		e.stateMu.Unlock()
		if gotAttempt != attempt {
			t.Fatalf("restart attempt count=%d, want %d", gotAttempt, attempt)
		}
		e.stateMu.Lock()
		e.nextRestart = time.Now().Add(time.Hour)
		e.stateMu.Unlock()
		if _, err = host.Resolve(context.Background(), "unstable", input, nil); err == nil {
			t.Fatal("adapter restarted during forced backoff")
		}
		wantCount := strings.Repeat("x", attempt+1)
		if data, readErr := os.ReadFile(marker); readErr != nil || string(data) != wantCount {
			t.Fatalf("spawn count after generation %d = %q, want %q (err %v)", attempt, data, wantCount, readErr)
		}
	}
}

func TestRestartBackoffGrowsAndCapsDeterministically(t *testing.T) {
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 25_600 * time.Millisecond}
	for i, expected := range want {
		attempt := i + 1
		if attempt == len(want) {
			attempt = 20
		}
		if got := restartBackoff(attempt); got != expected {
			t.Fatalf("restartBackoff(%d)=%v, want %v", attempt, got, expected)
		}
	}
	now := time.Unix(1_000, 0)
	for attempt := 1; attempt <= 4; attempt++ {
		e := &entry{status: Status{RestartAttempts: attempt - 1}}
		scheduleRestartLocked(e, nil, now)
		if got, expected := e.nextRestart.Sub(now), restartBackoff(attempt); got != expected {
			t.Fatalf("scheduled delay at attempt %d=%v, want %v", attempt, got, expected)
		}
	}
}

func TestFailedRestartAfterExpiredBackoffSchedulesAnotherDeadline(t *testing.T) {
	e := &entry{
		status:      Status{RestartAttempts: 1, State: "unavailable"},
		path:        filepath.Join(t.TempDir(), "missing-adapter"),
		nextRestart: time.Now().Add(-time.Second),
	}
	host := &Host{}
	if _, err := host.ensureProcess(context.Background(), e); err == nil {
		t.Fatal("missing adapter unexpectedly restarted")
	}
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if e.nextRestart.IsZero() || !e.nextRestart.After(time.Now()) {
		t.Fatalf("failed restart did not schedule backoff: %v", e.nextRestart)
	}
	if e.status.RestartAttempts != 2 {
		t.Fatalf("restart attempts = %d, want 2", e.status.RestartAttempts)
	}
}

func TestStableProcessUptimeResetsRestartBackoff(t *testing.T) {
	e := &entry{status: Status{RestartAttempts: 7}}
	p := &process{startedAt: time.Now().Add(-processStableUptime - time.Second)}
	now := time.Now()
	scheduleRestartLocked(e, p, now)
	if e.status.RestartAttempts != 0 || !e.nextRestart.Equal(now.Add(restartBackoff(1))) {
		t.Fatalf("stable process did not reset restart backoff: attempts=%d next=%v", e.status.RestartAttempts, e.nextRestart)
	}
}

func TestLegacyResolveRequiresEffectiveConfigurationButWorkflowCanChallenge(t *testing.T) {
	input := json.RawMessage(`{"manifest_url":"https://example.test/live.m3u8"}`)
	t.Run("missing required config blocks legacy IPC", func(t *testing.T) {
		dir := t.TempDir()
		marker := filepath.Join(dir, "resolve-called")
		writeAdapterWithMarker(t, dir, "required_config", "legacy", marker)
		host, err := Discover(context.Background(), dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer host.Close()
		if _, err = host.Resolve(context.Background(), "legacy", input, nil); err == nil || !strings.Contains(err.Error(), "required configuration key") {
			t.Fatalf("missing required config error = %v", err)
		}
		if _, err = os.Stat(marker); !os.IsNotExist(err) {
			t.Fatalf("legacy resolve IPC ran without required config: %v", err)
		}
	})
	t.Run("required config inherited from parent passes", func(t *testing.T) {
		root := t.TempDir()
		configStore, secretStore, err := pluginconfig.NewTypedFileStores(root)
		if err != nil {
			t.Fatal(err)
		}
		configs, err := pluginconfig.NewService(configStore, secretStore)
		if err != nil {
			t.Fatal(err)
		}
		dir := t.TempDir()
		marker := filepath.Join(dir, "resolve-called")
		writeAdapterWithMarker(t, dir, "required_inherited", "legacy", marker)
		host, err := Discover(context.Background(), dir, configs)
		if err != nil {
			t.Fatal(err)
		}
		defer host.Close()
		descriptor, err := host.Descriptor("legacy")
		if err != nil {
			t.Fatal(err)
		}
		parent := &adapterproto.ResourceRef{Type: "alpha", ID: "parent"}
		child := &adapterproto.ResourceRef{Type: "beta", ID: "child", Parent: parent}
		if err = configs.Put(pluginconfig.Scope{PluginID: "legacy", Resource: parent}, descriptor.ResourceTypes[0].ConfigurationSchema, map[string]json.RawMessage{"required_setting": json.RawMessage(`"inherited"`)}, nil); err != nil {
			t.Fatal(err)
		}
		if _, err = host.Resolve(context.Background(), "legacy", input, child); err != nil {
			t.Fatalf("inherited required config rejected: %v", err)
		}
		if data, err := os.ReadFile(marker); err != nil || string(data) != "x" {
			t.Fatalf("legacy resolve was not called with inherited config: %q, %v", data, err)
		}
	})
	t.Run("workflow may issue a generic challenge", func(t *testing.T) {
		dir := t.TempDir()
		writeAdapter(t, dir, binaryPrefix+"workflow", "workflow_required", "workflow", true)
		host, err := Discover(context.Background(), dir, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer host.Close()
		progress, err := host.BeginResolution(context.Background(), "workflow", input, nil)
		if err != nil || progress.State != "configuration_required" || progress.Challenge == nil {
			t.Fatalf("workflow challenge = %#v, %v", progress, err)
		}
	})
}

func TestAdvanceWorkflowPreservesContinuationReservationAcrossChallenge(t *testing.T) {
	const workflowID = "opaque-workflow"
	host := &Host{
		interactions: interaction.NewTracker(),
		workflows: map[string]workflowSession{
			workflowID: {adapterID: "adapter", continuing: true, continuationID: 41},
		},
	}
	challenge := &adapterproto.WorkflowChallenge{Schema: adapterproto.Schema{Fields: []adapterproto.Field{{Key: "answer", Control: "text", Label: "Answer"}}}}
	session := workflowSession{adapterID: "adapter", workflowID: workflowID, generation: 1, createdAt: time.Now().UTC(), updatedAt: time.Now().UTC(), continuing: true, continuationID: 41, transitions: 1}
	progress, err := host.advanceWorkflow(context.Background(), adapterproto.Descriptor{ID: "adapter"}, session, adapterproto.ResolveWorkflowResult{State: "configuration_required", WorkflowID: workflowID, Challenge: challenge})
	if err != nil || progress.Challenge == nil {
		t.Fatalf("advanced workflow = %#v, %v", progress, err)
	}
	session, exists := host.workflows[workflowID]
	if !exists || !session.continuing || session.continuationID != 41 {
		t.Fatalf("active continuation reservation was lost: %#v", session)
	}
}

func writeAdapterWithMarker(t *testing.T, dir, mode, id, marker string) {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nIR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=" + shellQuote(mode) + " IR_ADAPTER_ID=" + shellQuote(id) + " IR_ADAPTER_MARKER=" + shellQuote(marker) + " exec " + shellQuote(binary) + "\n"
	if err = os.WriteFile(filepath.Join(dir, binaryPrefix+id), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
}

func writeExitingAdapter(t *testing.T, dir, name string) {
	t.Helper()
	response := `{"protocol_version":1,"id":"1","result":{"id":"exit","name":"Exit adapter","version":"1","protocol_version":1,"capabilities":["resolve"],"input_schema":{"fields":[]},"configuration_schema":{"fields":[]},"resource_types":[],"media_types":["hls"]}}`
	script := "#!/bin/sh\nIFS= read -r request || exit 1\nprintf '%s\\n' '" + response + "'\n"
	if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
}

func writeAdapter(t *testing.T, dir, name, mode, id string, executable bool) {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf("#!/bin/sh\nIR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=%s IR_ADAPTER_ID=%s exec %s\n", shellQuote(mode), shellQuote(id), shellQuote(binary))
	path := filepath.Join(dir, name)
	permissions := os.FileMode(0600)
	if executable {
		permissions = 0700
	}
	if err = os.WriteFile(path, []byte(script), permissions); err != nil {
		t.Fatal(err)
	}
}

func writeRestartingAdapter(t *testing.T, dir, id, firstMode, laterID, laterMode string) {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "started")
	script := "#!/bin/sh\nif [ ! -f " + shellQuote(marker) + " ]; then : > " + shellQuote(marker) + "; IR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=" + shellQuote(firstMode) + " IR_ADAPTER_ID=" + shellQuote(id) + " exec " + shellQuote(binary) + "; fi\nIR_ADAPTER_HELPER=1 IR_ADAPTER_MODE=" + shellQuote(laterMode) + " IR_ADAPTER_ID=" + shellQuote(laterID) + " exec " + shellQuote(binary) + "\n"
	if err = os.WriteFile(filepath.Join(dir, binaryPrefix+id), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
}
func shellQuote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'" }

func TestWatchCheckUsesSeparatedInputSecretAndLegacyResolveKeepsInlineSecret(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, binaryPrefix+"watch", "watch", "watch", true)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()

	input := json.RawMessage(`{"manifest_url":"https://watch.example/live.m3u8"}`)
	if err := host.ValidateWatchInput("watch", input, map[string]string{"token": "split-watch-secret"}, nil); err != nil {
		t.Fatalf("valid separated Watch input rejected: %v", err)
	}
	result, err := host.WatchCheck(context.Background(), "watch", input, map[string]string{"token": "split-watch-secret"}, nil)
	if err != nil {
		t.Fatalf("WatchCheck: %v", err)
	}
	if result.State != "live" || result.Media == nil || result.Media.ManifestURL != "https://watch.example/live.m3u8" {
		t.Fatalf("WatchCheck result = %#v", result)
	}
	if err := host.ValidateWatchInput("watch", json.RawMessage(`{"manifest_url":"https://watch.example/live.m3u8","token":"inline"}`), nil, nil); err == nil {
		t.Fatal("Watch accepted a secret in ordinary input")
	}

	legacyInput := json.RawMessage(`{"manifest_url":"https://legacy.example/live.m3u8","token":"inline-secret"}`)
	media, err := host.ResolveLegacy(context.Background(), "watch", legacyInput, nil)
	if err != nil {
		t.Fatalf("legacy Resolve rejected inline schema secret: %v", err)
	}
	if media.ManifestURL != "https://legacy.example/live.m3u8" {
		t.Fatalf("legacy resolve media = %#v", media)
	}
}

func TestPrepareMetadataUsesEffectiveContextAndDefersStateMutation(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, binaryPrefix+"metadata", "metadata", "metadata", true)
	configStore, secretStore, state, err := pluginconfig.NewTypedFileStoresAndState(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	configs, err := pluginconfig.NewService(configStore, secretStore)
	if err != nil {
		t.Fatal(err)
	}
	host, err := DiscoverWithState(context.Background(), dir, configs, state)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	descriptor, err := host.Descriptor("metadata")
	if err != nil {
		t.Fatal(err)
	}
	if err = configs.Put(pluginconfig.Scope{PluginID: "metadata"}, descriptor.ConfigurationSchema,
		map[string]json.RawMessage{"label": json.RawMessage(`"stored"`)},
		map[string]string{"token": "private-config-secret"}); err != nil {
		t.Fatal(err)
	}
	if err = state.Apply([]pluginconfig.StateMutation{{Scope: pluginconfig.Scope{PluginID: "metadata"}, Secrets: map[string]string{"session": "private-state-secret"}}}); err != nil {
		t.Fatal(err)
	}
	resource := &adapterproto.ResourceRef{Type: "alpha", ID: "child"}
	current := adapterproto.MediaSource{Type: "hls", ManifestURL: "https://current.example/live.m3u8"}
	result, commit, supported, err := host.PrepareMetadata(context.Background(), "metadata", resource, current)
	if err != nil || !supported || result.Metadata.Title == nil || *result.Metadata.Title != "fixture title" || result.Metadata.Description == nil || *result.Metadata.Description != "" || commit == nil {
		var title string
		if result.Metadata.Title != nil {
			title = *result.Metadata.Title
		}
		t.Fatalf("prepared metadata=%#v title=%q supported=%v err=%v", result, title, supported, err)
	}
	doc, err := state.Get(pluginconfig.Scope{PluginID: "metadata"})
	if err != nil || doc.Secrets["session"] != "private-state-secret" {
		t.Fatalf("metadata state mutation committed before canonical caller: %#v, %v", doc.Secrets, err)
	}
	if err := commit(); err != nil {
		t.Fatal(err)
	}
	doc, err = state.Get(pluginconfig.Scope{PluginID: "metadata"})
	if err != nil || doc.Secrets["session"] != "metadata-updated-secret" {
		t.Fatalf("metadata state mutation was not applied: %#v, %v", doc.Secrets, err)
	}

	entry, err := host.entryFor("metadata")
	if err != nil {
		t.Fatal(err)
	}
	entry.stateMu.Lock()
	process := entry.process
	entry.stateMu.Unlock()
	process.kill()
	if _, _, supported, err = host.PrepareMetadata(context.Background(), "metadata", resource, current); err != nil || !supported {
		t.Fatalf("metadata call after adapter restart: supported=%v err=%v", supported, err)
	}
}

func TestPrepareMetadataReportsUnsupportedCapabilityWithoutCallingAdapter(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, binaryPrefix+"plain", "normal", "plain", true)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	_, _, supported, err := host.PrepareMetadata(context.Background(), "plain", nil, adapterproto.MediaSource{Type: "hls", ManifestURL: "https://plain.example/live.m3u8"})
	if err != nil || supported {
		t.Fatalf("metadata unsupported result supported=%v err=%v", supported, err)
	}
}

func TestWatchCheckRejectsAdapterWithoutCapability(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, binaryPrefix+"plain", "normal", "plain", true)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if _, err := host.WatchCheck(context.Background(), "plain", json.RawMessage(`{"manifest_url":"https://plain.example/live.m3u8"}`), nil, nil); err == nil || !strings.Contains(err.Error(), "does not support watch") {
		t.Fatalf("WatchCheck without capability error = %v", err)
	}
}

func TestWatchCheckPreservesOnlyAllowlistedProtocolErrorCodes(t *testing.T) {
	for _, test := range []struct {
		name      string
		wantCode  string
		wantTyped bool
	}{
		{name: "authentication", wantCode: "authentication_required", wantTyped: true},
		{name: "interaction", wantCode: "interaction_required", wantTyped: true},
		{name: "configuration", wantCode: "configuration_required", wantTyped: true},
		{name: "disallowed", wantCode: "remote_failure_secret_sentinel"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			adapterID := "watch-error-" + test.name
			writeAdapter(t, dir, binaryPrefix+adapterID, "watch_error_"+test.wantCode, adapterID, true)
			host, err := Discover(context.Background(), dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer host.Close()

			_, err = host.WatchCheck(context.Background(), adapterID, json.RawMessage(`{"manifest_url":"https://watch.example/live.m3u8"}`), nil, nil)
			if err == nil || err.Error() != "adapter returned an error" {
				t.Fatalf("WatchCheck error = %v; want stable generic message", err)
			}
			if strings.Contains(err.Error(), "secret-sentinel") {
				t.Fatalf("remote error message/details leaked: %v", err)
			}
			var safeErr *SafeProtocolError
			if test.wantTyped {
				if !errors.As(err, &safeErr) || safeErr.Code != test.wantCode {
					t.Fatalf("typed protocol error = %#v; want %q", safeErr, test.wantCode)
				}
			} else if errors.As(err, &safeErr) {
				t.Fatalf("disallowed remote code escaped as typed error: %#v", safeErr)
			}
		})
	}
}
