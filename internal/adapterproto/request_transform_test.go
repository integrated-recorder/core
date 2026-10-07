package adapterproto

import (
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestApplyURLTransformPropagatesSelectedQueryAndRewritesPathSuffix(t *testing.T) {
	policy := &URLTransformPolicy{Rules: []URLTransformRule{{
		Scopes:          []ResourceRequestScope{RequestScopeMedia},
		PathSuffix:      &PathSuffixRewrite{From: ".ts", To: ".m4v"},
		QueryParameters: []QueryParameterPropagation{{From: "hdnts", To: "__bgda__"}},
	}}}
	got, err := ApplyURLTransform(
		"https://stream.example/master?hdnts=token%2Bvalue&ignored=x",
		"https://stream.example/chunk.ts?keep=yes",
		RequestScopeMedia,
		policy,
	)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Path != "/chunk.m4v" {
		t.Fatalf("rewritten path = %q", parsed.Path)
	}
	query := parsed.Query()
	if query.Get("__bgda__") != "token+value" || query.Get("keep") != "yes" || query.Get("ignored") != "" {
		t.Fatalf("transformed query = %v", query)
	}
}

func TestApplyURLTransformSupportsExtensionlessAndQueryManifestURLs(t *testing.T) {
	policy := &URLTransformPolicy{Rules: []URLTransformRule{{
		Scopes:          []ResourceRequestScope{RequestScopeManifest},
		QueryParameters: []QueryParameterPropagation{{From: "ticket", To: "auth"}},
	}}}
	got, err := ApplyURLTransform("https://stream.example/manifest?ticket=abc", "https://stream.example/live?id=1", RequestScopeManifest, policy)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(got)
	if parsed.Path != "/live" || parsed.Query().Get("id") != "1" || parsed.Query().Get("auth") != "abc" {
		t.Fatalf("transformed extensionless URL = %q", got)
	}
}

func TestApplyURLTransformFiltersByScopeAndPreservesOrigin(t *testing.T) {
	policy := &URLTransformPolicy{Rules: []URLTransformRule{{
		Scopes:          []ResourceRequestScope{RequestScopeKey},
		PathSuffix:      &PathSuffixRewrite{From: ".key", To: ".bin"},
		QueryParameters: []QueryParameterPropagation{{From: "token", To: "auth"}},
	}}}
	input := "https://stream.example/encryption.key?keep=1"
	got, err := ApplyURLTransform("https://stream.example/master?token=secret", input, RequestScopeMedia, policy)
	if err != nil {
		t.Fatal(err)
	}
	if got != input {
		t.Fatalf("out-of-scope URL changed: %q", got)
	}
	got, err = ApplyURLTransform("https://stream.example/master?token=secret", input, RequestScopeKey, policy)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(got)
	if parsed.Scheme != "https" || parsed.Host != "stream.example" || parsed.Path != "/encryption.bin" || parsed.Query().Get("auth") != "secret" {
		t.Fatalf("transformed URL = %q", got)
	}
}

func TestApplyURLTransformRequiresExplicitCrossOriginAllowance(t *testing.T) {
	policy := &URLTransformPolicy{Rules: []URLTransformRule{{
		Scopes:          []ResourceRequestScope{RequestScopeMedia},
		QueryParameters: []QueryParameterPropagation{{From: "hdnts", To: "__bgda__"}},
	}}}
	base := "https://stream.example/master?hdnts=secret"
	target := "https://cdn.example/chunk.m4s"
	if _, err := ApplyURLTransform(base, target, RequestScopeMedia, policy); err == nil {
		t.Fatal("cross-origin query propagation accepted without allowance")
	}
	policy.AllowedOrigins = []string{"https://cdn.example"}
	got, err := ApplyURLTransform(base, target, RequestScopeMedia, policy)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(got)
	if parsed.Host != "cdn.example" || parsed.Query().Get("__bgda__") != "secret" {
		t.Fatalf("allowed cross-origin transform = %q", got)
	}
	policy.AllowedOrigins = []string{"https://other.example"}
	if _, err := ApplyURLTransform(base, target, RequestScopeMedia, policy); err == nil {
		t.Fatal("unlisted cross-origin target accepted")
	}
}

func TestURLTransformValidationBoundsAndRejectsAmbiguousRules(t *testing.T) {
	valid := URLTransformPolicy{Rules: []URLTransformRule{{Scopes: []ResourceRequestScope{RequestScopeMedia}, QueryParameters: []QueryParameterPropagation{{From: "hdnts", To: "__bgda__"}}}}}
	duplicateRule := URLTransformRule{Scopes: []ResourceRequestScope{RequestScopeMedia}, PathSuffix: &PathSuffixRewrite{From: ".ts", To: ".m4v"}}
	invalid := []URLTransformPolicy{
		{Rules: make([]URLTransformRule, maxURLTransformRules+1)},
		{AllowedOrigins: []string{"https://a.example", "https://a.example"}},
		{AllowedOrigins: make([]string, maxURLTransformOrigins+1)},
		{AllowedOrigins: []string{"https://a.example/path"}},
		{Rules: []URLTransformRule{{Scopes: []ResourceRequestScope{"unknown"}, PathSuffix: &PathSuffixRewrite{From: ".ts", To: ".m4v"}}}},
		{Rules: []URLTransformRule{{Scopes: []ResourceRequestScope{RequestScopeMedia, RequestScopeMedia}, PathSuffix: &PathSuffixRewrite{From: ".ts", To: ".m4v"}}}},
		{Rules: []URLTransformRule{{Scopes: []ResourceRequestScope{RequestScopeMedia}, PathSuffix: &PathSuffixRewrite{From: strings.Repeat("x", maxURLTransformSuffixBytes+1), To: ".m4v"}}}},
		{Rules: []URLTransformRule{{Scopes: []ResourceRequestScope{RequestScopeMedia}, PathSuffix: &PathSuffixRewrite{From: ".ts", To: "../escape"}}}},
		{Rules: []URLTransformRule{{Scopes: []ResourceRequestScope{RequestScopeMedia}, QueryParameters: []QueryParameterPropagation{{From: "bad name", To: "auth"}}}}},
		{Rules: []URLTransformRule{{Scopes: []ResourceRequestScope{RequestScopeMedia}, QueryParameters: []QueryParameterPropagation{{From: "token", To: "auth"}, {From: "hdnts", To: "auth"}}}}},
		{Rules: []URLTransformRule{{Scopes: []ResourceRequestScope{RequestScopeMedia}, QueryParameters: make([]QueryParameterPropagation, maxURLTransformQueryCopies+1)}}},
		{Rules: []URLTransformRule{duplicateRule, duplicateRule}},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
	for i, candidate := range invalid {
		if err := candidate.Validate(); err == nil {
			t.Errorf("invalid policy %d accepted", i)
		}
	}
	for _, raw := range []string{
		"file:///tmp/chunk.ts",
		"https://user:pass@stream.example/chunk.ts",
		"https://stream.example:bad/chunk.ts",
		"https://stream.example/a\\b",
		"https://stream.example/" + strings.Repeat("x", maxMediaRequestURLBytes),
		string([]byte{0xff}),
		"https://stream.example/path\nsegment",
	} {
		if _, err := ApplyURLTransform("https://stream.example/master", raw, RequestScopeMedia, &valid); err == nil {
			t.Errorf("unsafe or oversized target URL accepted: %q", raw)
		}
	}
}

func TestURLTransformRejectsDuplicateQueryValuesAndDoesNotLeakAcrossOrigins(t *testing.T) {
	policy := &URLTransformPolicy{Rules: []URLTransformRule{{Scopes: []ResourceRequestScope{RequestScopeMedia}, QueryParameters: []QueryParameterPropagation{{From: "token", To: "auth"}}}}}
	for _, base := range []string{
		"https://stream.example/master?token=a&token=b",
		"https://stream.example/master?token=%00",
	} {
		if _, err := ApplyURLTransform(base, "https://stream.example/segment", RequestScopeMedia, policy); err == nil {
			t.Errorf("ambiguous/control query accepted: %q", base)
		}
	}
}

func TestApplyURLTransformRejectsAmbiguousSourceQuery(t *testing.T) {
	policy := &URLTransformPolicy{Rules: []URLTransformRule{{Scopes: []ResourceRequestScope{RequestScopeMedia}, QueryParameters: []QueryParameterPropagation{{From: "token", To: "auth"}}}}}
	if _, err := ApplyURLTransform("https://stream.example/master?token=a&token=b", "https://stream.example/seg", RequestScopeMedia, policy); err == nil {
		t.Fatal("duplicate source query values were accepted")
	}
}

func TestHistoricalAvailabilityValidation(t *testing.T) {
	valid := []HistoricalAvailability{
		{Mode: HistoricalModeRollingWindow, WindowSeconds: 3600, HistoricalManifestURL: "https://stream.example/archive?id=1"},
		{Mode: HistoricalModeSequenceRanges, SequenceRanges: []HistoricalSequenceRange{{Start: 10, End: 20}, {Start: 30, End: 40}}},
		{Mode: HistoricalModeTimeRanges, TimeRanges: []HistoricalTimeRange{{Start: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)}}},
		{Mode: HistoricalModeManifest, HistoricalManifestURL: "https://stream.example/archive/index.m3u8?ticket=opaque"},
	}
	for i := range valid {
		if err := valid[i].Validate(); err != nil {
			t.Errorf("valid historical declaration %d rejected: %v", i, err)
		}
	}
	invalid := []HistoricalAvailability{
		{Mode: HistoricalModeRollingWindow},
		{Mode: HistoricalModeRollingWindow, WindowSeconds: maxHistoricalWindowSeconds + 1},
		{Mode: HistoricalModeSequenceRanges, SequenceRanges: []HistoricalSequenceRange{{Start: 10, End: 20}, {Start: 20, End: 30}}},
		{Mode: HistoricalModeSequenceRanges, SequenceRanges: []HistoricalSequenceRange{{Start: 20, End: 10}}},
		{Mode: HistoricalModeTimeRanges, TimeRanges: []HistoricalTimeRange{{Start: time.Unix(2, 0), End: time.Unix(1, 0)}}},
		{Mode: HistoricalModeSequenceRanges, SequenceRanges: make([]HistoricalSequenceRange, maxHistoricalRanges+1)},
		{Mode: HistoricalModeRollingWindow, WindowSeconds: 1, SequenceRanges: []HistoricalSequenceRange{{Start: 1, End: 1}}},
		{Mode: HistoricalModeRollingWindow, WindowSeconds: 1, HistoricalManifestURL: "file:///tmp/archive.m3u8"},
		{Mode: HistoricalModeManifest},
		{Mode: HistoricalModeManifest, HistoricalManifestURL: "https://stream.example/archive", WindowSeconds: 1},
		{Mode: HistoricalModeManifest, HistoricalManifestURL: "https://stream.example/archive", SequenceRanges: []HistoricalSequenceRange{{Start: 1, End: 1}}},
		{Mode: HistoricalModeManifest, HistoricalManifestURL: "https://stream.example/archive", TimeRanges: []HistoricalTimeRange{{Start: time.Unix(1, 0), End: time.Unix(2, 0)}}},
		{Mode: "unknown"},
	}
	for i := range invalid {
		if err := invalid[i].Validate(); err == nil {
			t.Errorf("invalid historical declaration %d accepted", i)
		}
	}
}

func TestHistoricalManifestAvailabilityJSONContract(t *testing.T) {
	const want = `{"mode":"manifest","historical_manifest_url":"https://stream.example/archive/index.m3u8?ticket=opaque"}`
	var availability HistoricalAvailability
	if err := json.Unmarshal([]byte(want), &availability); err != nil {
		t.Fatal(err)
	}
	if err := availability.Validate(); err != nil {
		t.Fatalf("manifest vector rejected: %v", err)
	}
	wire, err := json.Marshal(availability)
	if err != nil {
		t.Fatal(err)
	}
	if string(wire) != want {
		t.Fatalf("manifest wire = %s, want %s", wire, want)
	}
}

func TestSDKManifestGoldenResponsePassesCoreValidation(t *testing.T) {
	data, err := os.ReadFile("../../protocol/adapter-v1/resolve-historical-manifest.response.json")
	if err != nil {
		t.Fatal(err)
	}
	frame, err := ParseFrame(data)
	if err != nil {
		t.Fatalf("SDK manifest golden response rejected: %v", err)
	}
	if frame.Response == nil || frame.Response.ID != "resolve-historical-manifest" {
		t.Fatalf("unexpected SDK golden frame: %#v", frame)
	}
	var media MediaSource
	if err := json.Unmarshal(frame.Response.Result, &media); err != nil {
		t.Fatal(err)
	}
	if err := ValidateMediaSource(media, []string{"hls"}); err != nil {
		t.Fatalf("Core rejected SDK manifest media contract: %v", err)
	}
}

func TestMediaSourceOptionalFieldsAreBackwardCompatible(t *testing.T) {
	const oldWire = `{"type":"hls","manifest_url":"https://stream.example/live"}`
	var media MediaSource
	if err := json.Unmarshal([]byte(oldWire), &media); err != nil {
		t.Fatal(err)
	}
	if err := ValidateMediaSource(media, []string{"hls"}); err != nil {
		t.Fatalf("legacy media source rejected: %v", err)
	}
	wire, err := json.Marshal(media)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), "historical_availability") || strings.Contains(string(wire), "url_transform") {
		t.Fatalf("omitted optional fields appeared on legacy round trip: %s", wire)
	}

	media.HistoricalAvailability = &HistoricalAvailability{Mode: HistoricalModeRollingWindow, WindowSeconds: 3600}
	media.RequestPolicy = &RequestPolicy{URLTransform: &URLTransformPolicy{Rules: []URLTransformRule{{Scopes: []ResourceRequestScope{RequestScopeMedia}, PathSuffix: &PathSuffixRewrite{From: ".ts", To: ".m4v"}}}}}
	if err := ValidateMediaSource(media, []string{"hls"}); err != nil {
		t.Fatalf("valid additive fields rejected: %v", err)
	}
	wire, err = json.Marshal(media)
	if err != nil {
		t.Fatal(err)
	}
	var decoded MediaSource
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := ValidateMediaSource(decoded, []string{"hls"}); err != nil {
		t.Fatalf("new media source wire shape rejected: %v", err)
	}
}

func TestValidateMediaSourceValidatesHistoricalAndTransformDeclarations(t *testing.T) {
	media := MediaSource{Type: "hls", ManifestURL: "https://stream.example/live"}
	media.HistoricalAvailability = &HistoricalAvailability{Mode: HistoricalModeSequenceRanges, SequenceRanges: []HistoricalSequenceRange{{Start: 1, End: 2}}}
	media.RequestPolicy = &RequestPolicy{URLTransform: &URLTransformPolicy{Rules: []URLTransformRule{{Scopes: []ResourceRequestScope{RequestScopeMedia}, PathSuffix: &PathSuffixRewrite{From: ".ts", To: ".m4v"}}}}}
	if err := ValidateMediaSource(media, []string{"hls"}); err != nil {
		t.Fatalf("valid extended media source rejected: %v", err)
	}
	media.HistoricalAvailability.SequenceRanges[0].Start = 3
	if err := ValidateMediaSource(media, []string{"hls"}); err == nil {
		t.Fatal("invalid historical coverage declaration accepted")
	}
	media.HistoricalAvailability = nil
	media.RequestPolicy.URLTransform.Rules[0].PathSuffix.To = "../outside"
	if err := ValidateMediaSource(media, []string{"hls"}); err == nil {
		t.Fatal("invalid URL transform declaration accepted")
	}
}
