package owncast

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

func TestResolveValidURLAndRemovesQueryAndFragment(t *testing.T) {
	media, err := Resolve(json.RawMessage(`{"source_url":"https://live.example/custom/base/?token=ignored#section"}`))
	if err != nil {
		t.Fatal(err)
	}
	if media.Type != "hls" || media.ManifestURL != "https://live.example/custom/base/hls/stream.m3u8" {
		t.Fatalf("media = %#v", media)
	}
}

func TestResolveRejectsInvalidURL(t *testing.T) {
	for _, input := range []string{`{"source_url":""}`, `{"source_url":"file:///tmp/live"}`, `{"source_url":"https:///missing-host"}`, `{"source_url":"https://user:pass@live.example"}`, `{"source_url":"//live.example"}`} {
		if _, err := Resolve(json.RawMessage(input)); err == nil {
			t.Errorf("Resolve(%s) succeeded", input)
		}
	}
}

func TestDescribeSchemaIsGenericAndValid(t *testing.T) {
	d := Describe()
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
	if d.ID != "owncast" || d.ProtocolVersion != adapterproto.Version || len(d.InputSchema.Fields) != 1 || d.InputSchema.Fields[0].Key != "source_url" || len(d.ConfigurationSchema.Fields) != 0 {
		t.Fatalf("descriptor = %#v", d)
	}
	if d.Branding == nil || d.Branding.Icon == nil || d.Branding.Icon.MediaType != "image/png" || len(d.Branding.Icon.Data) == 0 {
		t.Fatalf("Owncast descriptor branding = %#v", d.Branding)
	}
}

func TestWatchCheckUsesStatusAPIAndReturnsMediaFastPath(t *testing.T) {
	var seenPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPath = r.URL.Path
		if r.Method != http.MethodGet {
			t.Errorf("method = %s", r.Method)
		}
		_, _ = io.WriteString(w, `{"online":true,"lastConnectTime":"2026-09-28T01:02:03Z","streamTitle":"Broadcast title"}`)
	}))
	defer server.Close()
	validated := ""
	deadlineSeen := false
	result, err := WatchCheckWith(json.RawMessage(`{"source_url":"`+server.URL+`/"}`), server.Client(), func(ctx context.Context, raw string) error {
		validated = raw
		_, deadlineSeen = ctx.Deadline()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seenPath != statusPath || !strings.HasSuffix(validated, statusPath) || !deadlineSeen {
		t.Fatalf("status request path=%q validated=%q deadline=%v", seenPath, validated, deadlineSeen)
	}
	if result.State != "live" || result.Media == nil || result.Media.ManifestURL != server.URL+StreamPath || result.SessionRef != "2026-09-28T01:02:03Z" || result.StartedAt == nil {
		t.Fatalf("live result = %#v", result)
	}
	if result.Title != "Broadcast title" {
		t.Fatalf("initial Watch title = %q", result.Title)
	}
}

func TestMetadataUsesStatusStreamTitleAndPreservesKnownEmpty(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != statusPath {
			t.Errorf("request path = %q", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"online":true,"streamTitle":""}`)
	}))
	defer server.Close()
	result, err := MetadataWith(adapterproto.MediaSource{Type: "hls", ManifestURL: server.URL + StreamPath}, server.Client(), func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || result.Metadata.Title == nil || *result.Metadata.Title != "" || result.Metadata.Description != nil || result.SourceUpdatedAt != nil {
		t.Fatalf("metadata=%#v calls=%d", result, calls)
	}
	if !slices.Contains(Describe().Capabilities, adapterproto.CapabilityMetadata) {
		t.Fatal("Owncast descriptor does not declare metadata capability")
	}
}

func TestOversizedOptionalStreamTitleDoesNotTurnLiveWatchIntoFailure(t *testing.T) {
	title := strings.Repeat("x", 4<<10+1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"online": true, "streamTitle": title})
	}))
	defer server.Close()
	input := json.RawMessage(`{"source_url":"` + server.URL + `"}`)
	validate := func(context.Context, string) error { return nil }
	result, err := WatchCheckWith(input, server.Client(), validate)
	if err != nil || result.State != "live" || result.Media == nil || result.Title != "" {
		t.Fatalf("invalid optional title blocked live detection: result=%#v err=%v", result, err)
	}
	if _, err := MetadataWith(*result.Media, server.Client(), validate); err == nil {
		t.Fatal("oversized source metadata title was accepted")
	}
}

func TestWatchCheckOfflineAndRejectsStatusErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		code int
		body string
		want string
	}{
		{name: "offline", code: http.StatusOK, body: `{"online":false}`, want: "offline"},
		{name: "missing online", code: http.StatusOK, body: `{"streaming":false}`},
		{name: "malformed json", code: http.StatusOK, body: `{`},
		{name: "invalid identity", code: http.StatusOK, body: `{"online":true,"lastConnectTime":"not-a-time"}`},
		{name: "server error", code: http.StatusServiceUnavailable, body: `{"online":false}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.code)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			result, err := WatchCheckWith(json.RawMessage(`{"source_url":"`+server.URL+`"}`), server.Client(), func(context.Context, string) error { return nil })
			if test.want == "offline" {
				if err != nil || result.State != "offline" || result.Media != nil {
					t.Fatalf("offline result=%#v error=%v", result, err)
				}
				return
			}
			if err == nil || result.State == "offline" {
				t.Fatalf("invalid status was treated as offline: result=%#v error=%v", result, err)
			}
		})
	}
}

func TestWatchCheckAuthenticationStatusMapsToSafeProtocolFailure(t *testing.T) {
	for _, statusCode := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(statusCode)
				_, _ = io.WriteString(w, "response-body-secret-sentinel")
			}))
			defer server.Close()
			result, err := WatchCheckWith(json.RawMessage(`{"source_url":"`+server.URL+`"}`), server.Client(), func(context.Context, string) error { return nil })
			if err == nil || result.State == "offline" {
				t.Fatalf("authentication failure was treated as offline: result=%#v error=%v", result, err)
			}
			var checkErr *watchCheckError
			if !errors.As(err, &checkErr) || checkErr.code != "authentication_required" {
				t.Fatalf("WatchCheck error = %#v; want safe authentication code", err)
			}
			response := watchCheckFailure("request-1", err)
			if response.Error == nil || response.Error.Code != "authentication_required" || response.Error.Message != "status check failed" || response.Error.Details != nil {
				t.Fatalf("wire error = %#v", response.Error)
			}
			encoded, marshalErr := json.Marshal(response)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if strings.Contains(string(encoded), "secret-sentinel") || strings.Contains(string(encoded), server.URL) {
				t.Fatalf("authentication response leaked body or source URL: %s", encoded)
			}
		})
	}
}

func TestWatchCheckRejectsOversizedStatusBodyAndUsesBoundedValidationContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat("x", maxStatusBytes+1))
	}))
	defer server.Close()
	result, err := WatchCheckWith(json.RawMessage(`{"source_url":"`+server.URL+`"}`), server.Client(), func(ctx context.Context, _ string) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 9*time.Second {
			t.Fatalf("validation context deadline missing or unbounded: %v %v", deadline, ok)
		}
		return nil
	})
	if err == nil || result.State == "offline" {
		t.Fatalf("oversized status result=%#v error=%v", result, err)
	}
}
