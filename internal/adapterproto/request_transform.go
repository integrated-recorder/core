package adapterproto

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxMediaRequestURLBytes    = 8192
	maxURLTransformRules       = 16
	maxURLTransformScopes      = 5
	maxURLTransformQueryCopies = 8
	maxURLTransformOrigins     = 16
	maxURLTransformSuffixBytes = 128
	maxURLTransformValueBytes  = 4096
	maxHistoricalRanges        = 64
	maxHistoricalWindowSeconds = 366 * 24 * 60 * 60
)

type ResourceRequestScope string

const (
	RequestScopeManifest ResourceRequestScope = "manifest"
	RequestScopeVariant  ResourceRequestScope = "variant"
	RequestScopeMedia    ResourceRequestScope = "media"
	RequestScopeInit     ResourceRequestScope = "init"
	RequestScopeKey      ResourceRequestScope = "key"
)

// URLTransformPolicy contains literal, bounded request URL transformations.
// Core still owns URL resolution, host/network checks, and SSRF policy.
type URLTransformPolicy struct {
	// AllowedOrigins permits applying query/path rules to targets whose origin
	// differs from the base URL. The base origin is always allowed.
	AllowedOrigins []string           `json:"allowed_origins,omitempty"`
	Rules          []URLTransformRule `json:"rules,omitempty"`
}

// URLTransformRule applies only to the named request resource scopes.
type URLTransformRule struct {
	Scopes          []ResourceRequestScope      `json:"scopes"`
	PathSuffix      *PathSuffixRewrite          `json:"path_suffix,omitempty"`
	QueryParameters []QueryParameterPropagation `json:"query_parameters,omitempty"`
}

// PathSuffixRewrite replaces a literal suffix at the end of the URL path.
// It cannot add or remove a path separator or URL authority component.
type PathSuffixRewrite struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// QueryParameterPropagation copies one selected value from the base/source
// URL query to a named parameter on the target URL. Missing source values are
// left missing; ambiguous duplicate source values are rejected at application.
type QueryParameterPropagation struct {
	From string `json:"from"`
	To   string `json:"to"`
}

var safeQueryParameterName = regexp.MustCompile(`^[A-Za-z0-9_.~-]{1,128}$`)

// ApplyURLTransform applies a validated declarative transform to targetURL.
// Query values are copied from baseURL. The target must be same-origin with
// baseURL unless its exact HTTP(S) origin is listed in AllowedOrigins. The
// helper never changes the target scheme, host, port, or userinfo. Core must
// still run its authoritative network and SSRF validation on the result.
func ApplyURLTransform(baseURL, targetURL string, scope ResourceRequestScope, policy *URLTransformPolicy) (string, error) {
	base, err := parseTransformURL(baseURL)
	if err != nil {
		return "", fmt.Errorf("invalid transform base URL")
	}
	target, err := parseTransformURL(targetURL)
	if err != nil {
		return "", fmt.Errorf("invalid transform target URL")
	}
	if !validRequestScope(scope) {
		return "", fmt.Errorf("invalid request resource scope")
	}
	if policy == nil {
		return targetURL, nil
	}
	if err := policy.Validate(); err != nil {
		return "", fmt.Errorf("invalid URL transform policy: %w", err)
	}

	matching := make([]URLTransformRule, 0, len(policy.Rules))
	for _, rule := range policy.Rules {
		for _, candidate := range rule.Scopes {
			if candidate == scope {
				matching = append(matching, rule)
				break
			}
		}
	}
	if len(matching) == 0 {
		return targetURL, nil
	}
	baseOrigin, err := originFromURL(base)
	if err != nil {
		return "", fmt.Errorf("invalid transform base origin")
	}
	targetOrigin, err := originFromURL(target)
	if err != nil {
		return "", fmt.Errorf("invalid transform target origin")
	}
	if !baseOrigin.equal(targetOrigin) && !policy.allowsOrigin(targetOrigin) {
		return "", fmt.Errorf("URL transform target origin is not allowed")
	}

	baseQuery, err := url.ParseQuery(base.RawQuery)
	if err != nil {
		return "", fmt.Errorf("invalid transform base query")
	}
	targetQuery, err := url.ParseQuery(target.RawQuery)
	if err != nil {
		return "", fmt.Errorf("invalid transform target query")
	}
	for _, rule := range matching {
		if rule.PathSuffix != nil && strings.HasSuffix(target.Path, rule.PathSuffix.From) {
			target.Path = strings.TrimSuffix(target.Path, rule.PathSuffix.From) + rule.PathSuffix.To
			target.RawPath = ""
		}
		for _, copyRule := range rule.QueryParameters {
			values := baseQuery[copyRule.From]
			if len(values) == 0 {
				continue
			}
			if len(values) != 1 || !validBoundedText(values[0], maxURLTransformValueBytes, true) {
				return "", fmt.Errorf("transform source query value is ambiguous or invalid")
			}
			targetQuery.Set(copyRule.To, values[0])
		}
	}
	target.RawQuery = targetQuery.Encode()
	result := target.String()
	if len(result) > maxMediaRequestURLBytes {
		return "", fmt.Errorf("transformed URL exceeds limit")
	}
	// A transform is declarative and must not be able to mutate the request's
	// authority, even if future rule types are added.
	resultURL, err := parseTransformURL(result)
	if err != nil {
		return "", fmt.Errorf("transformed URL is invalid")
	}
	resultOrigin, err := originFromURL(resultURL)
	if err != nil || !resultOrigin.equal(targetOrigin) {
		return "", fmt.Errorf("URL transform changed target origin")
	}
	return result, nil
}

func (policy *URLTransformPolicy) Validate() error {
	if policy == nil {
		return nil
	}
	if len(policy.Rules) > maxURLTransformRules || len(policy.AllowedOrigins) > maxURLTransformOrigins {
		return fmt.Errorf("URL transform policy exceeds limit")
	}
	seenOrigins := map[mediaOrigin]bool{}
	for _, raw := range policy.AllowedOrigins {
		origin, err := parseAllowedOrigin(raw)
		if err != nil {
			return fmt.Errorf("invalid URL transform allowed origin")
		}
		if seenOrigins[origin] {
			return fmt.Errorf("URL transform origins must be unique")
		}
		seenOrigins[origin] = true
	}
	seenRules := map[string]bool{}
	for _, rule := range policy.Rules {
		if len(rule.Scopes) == 0 || len(rule.Scopes) > maxURLTransformScopes || len(rule.QueryParameters) > maxURLTransformQueryCopies {
			return fmt.Errorf("URL transform rule exceeds limit")
		}
		if rule.PathSuffix == nil && len(rule.QueryParameters) == 0 {
			return fmt.Errorf("URL transform rule has no operation")
		}
		seenScopes := map[ResourceRequestScope]bool{}
		scopes := append([]ResourceRequestScope(nil), rule.Scopes...)
		for _, scope := range scopes {
			if !validRequestScope(scope) || seenScopes[scope] {
				return fmt.Errorf("URL transform scopes must be valid and unique")
			}
			seenScopes[scope] = true
		}
		sort.Slice(scopes, func(i, j int) bool { return scopes[i] < scopes[j] })
		if rule.PathSuffix != nil {
			if !validPathSuffix(rule.PathSuffix.From, false) || !validPathSuffix(rule.PathSuffix.To, true) {
				return fmt.Errorf("URL transform path suffix is invalid")
			}
		}
		seenTargets := map[string]bool{}
		seenPairs := map[string]bool{}
		copies := append([]QueryParameterPropagation(nil), rule.QueryParameters...)
		sort.Slice(copies, func(i, j int) bool {
			if copies[i].From == copies[j].From {
				return copies[i].To < copies[j].To
			}
			return copies[i].From < copies[j].From
		})
		for _, copyRule := range copies {
			if !safeQueryParameterName.MatchString(copyRule.From) || !safeQueryParameterName.MatchString(copyRule.To) {
				return fmt.Errorf("URL transform query parameter name is invalid")
			}
			if seenTargets[copyRule.To] || seenPairs[copyRule.From+"\x00"+copyRule.To] {
				return fmt.Errorf("URL transform query mappings must be unique")
			}
			seenTargets[copyRule.To] = true
			seenPairs[copyRule.From+"\x00"+copyRule.To] = true
		}
		var key strings.Builder
		for _, scope := range scopes {
			key.WriteString(string(scope))
			key.WriteByte(',')
		}
		key.WriteByte('|')
		if rule.PathSuffix != nil {
			key.WriteString(rule.PathSuffix.From)
			key.WriteByte('>')
			key.WriteString(rule.PathSuffix.To)
		}
		key.WriteByte('|')
		for _, copyRule := range copies {
			key.WriteString(copyRule.From)
			key.WriteByte('>')
			key.WriteString(copyRule.To)
			key.WriteByte(',')
		}
		if seenRules[key.String()] {
			return fmt.Errorf("URL transform rules must be unique")
		}
		seenRules[key.String()] = true
	}
	return nil
}

func (policy *URLTransformPolicy) allowsOrigin(origin mediaOrigin) bool {
	if policy == nil {
		return false
	}
	for _, raw := range policy.AllowedOrigins {
		allowed, err := parseAllowedOrigin(raw)
		if err == nil && allowed.equal(origin) {
			return true
		}
	}
	return false
}

func validRequestScope(scope ResourceRequestScope) bool {
	switch scope {
	case RequestScopeManifest, RequestScopeVariant, RequestScopeMedia, RequestScopeInit, RequestScopeKey:
		return true
	default:
		return false
	}
}

func validPathSuffix(value string, allowEmpty bool) bool {
	if value == "" {
		return allowEmpty
	}
	if !validBoundedText(value, maxURLTransformSuffixBytes, false) || strings.ContainsAny(value, `/\\?#%`) || value == "." || value == ".." {
		return false
	}
	for _, r := range value {
		if r < 0x21 || r > 0x7e {
			return false
		}
	}
	return true
}

func parseTransformURL(raw string) (*url.URL, error) {
	if !validBoundedText(raw, maxMediaRequestURLBytes, false) || strings.ContainsAny(raw, "\\\r\n\t ") {
		return nil, fmt.Errorf("URL is malformed")
	}
	u, err := url.ParseRequestURI(raw)
	if err != nil || u == nil || u.Opaque != "" || u.User != nil || u.Fragment != "" || u.RawFragment != "" {
		return nil, fmt.Errorf("URL is malformed")
	}
	if _, err := originFromURL(u); err != nil {
		return nil, err
	}
	return u, nil
}

func validBoundedText(value string, max int, allowEmpty bool) bool {
	if (!allowEmpty && value == "") || len(value) > max || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r == 0x7f || r < 0x20 {
			return false
		}
	}
	return true
}

const (
	HistoricalModeRollingWindow  = "rolling_window"
	HistoricalModeSequenceRanges = "sequence_ranges"
	HistoricalModeTimeRanges     = "time_ranges"
	HistoricalModeManifest       = "manifest"
)

// HistoricalAvailability declares what media the adapter's source may make
// available for backfill. It is descriptive data; Core owns acquisition and
// does not delegate media fetching to the adapter.
type HistoricalAvailability struct {
	Mode                  string                    `json:"mode"`
	WindowSeconds         int64                     `json:"window_seconds,omitempty"`
	SequenceRanges        []HistoricalSequenceRange `json:"sequence_ranges,omitempty"`
	TimeRanges            []HistoricalTimeRange     `json:"time_ranges,omitempty"`
	HistoricalManifestURL string                    `json:"historical_manifest_url,omitempty"`
}

type HistoricalSequenceRange struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

type HistoricalTimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

func (availability *HistoricalAvailability) Validate() error {
	if availability == nil {
		return nil
	}
	if availability.HistoricalManifestURL != "" {
		if _, err := parseTransformURL(availability.HistoricalManifestURL); err != nil {
			return fmt.Errorf("historical manifest URL is invalid")
		}
	}
	switch availability.Mode {
	case HistoricalModeRollingWindow:
		if availability.WindowSeconds < 1 || availability.WindowSeconds > maxHistoricalWindowSeconds || len(availability.SequenceRanges) != 0 || len(availability.TimeRanges) != 0 {
			return fmt.Errorf("rolling historical window is invalid")
		}
	case HistoricalModeSequenceRanges:
		if availability.WindowSeconds != 0 || len(availability.SequenceRanges) == 0 || len(availability.SequenceRanges) > maxHistoricalRanges || len(availability.TimeRanges) != 0 {
			return fmt.Errorf("historical sequence ranges are invalid")
		}
		for i, span := range availability.SequenceRanges {
			if span.Start > span.End || i > 0 && span.Start <= availability.SequenceRanges[i-1].End {
				return fmt.Errorf("historical sequence ranges must be ordered and non-overlapping")
			}
		}
	case HistoricalModeTimeRanges:
		if availability.WindowSeconds != 0 || len(availability.TimeRanges) == 0 || len(availability.TimeRanges) > maxHistoricalRanges || len(availability.SequenceRanges) != 0 {
			return fmt.Errorf("historical time ranges are invalid")
		}
		for i, span := range availability.TimeRanges {
			if span.Start.IsZero() || span.End.IsZero() || !span.Start.Before(span.End) || span.Start.Year() < 1 || span.Start.Year() > 9999 || span.End.Year() < 1 || span.End.Year() > 9999 {
				return fmt.Errorf("historical time range is invalid")
			}
			if i > 0 && !availability.TimeRanges[i-1].End.Before(span.Start) {
				return fmt.Errorf("historical time ranges must be ordered and non-overlapping")
			}
		}
	case HistoricalModeManifest:
		if availability.HistoricalManifestURL == "" || availability.WindowSeconds != 0 || len(availability.SequenceRanges) != 0 || len(availability.TimeRanges) != 0 {
			return fmt.Errorf("historical manifest availability is invalid")
		}
	default:
		return fmt.Errorf("unsupported historical availability mode")
	}
	return nil
}
