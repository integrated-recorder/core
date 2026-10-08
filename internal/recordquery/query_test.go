package recordquery

import (
	"encoding/base64"
	"encoding/json"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func fixtureItems() []Item {
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	return []Item{
		{ID: "z", Title: "Evening", AdapterID: "owncast", AdapterName: "Owncast", State: "completed", ResourceType: "alpha", ResourceID: "res-z", Tags: []string{"concert"}, StartedAt: base.Add(24 * time.Hour), CreatedAt: base, DurationSeconds: 10, ArchiveSizeBytes: ptrInt64(100), GapDurationSeconds: ptr(0.5), GapCount: 1, Integrity: "verified"},
		{ID: "b", Title: "Morning", AdapterID: "other", AdapterName: "Other Adapter", State: "stopped", ResourceType: "beta", ResourceID: "stable-b", Tags: []string{"important"}, StartedAt: base.Add(24 * time.Hour), CreatedAt: base.Add(48 * time.Hour), DurationSeconds: 30, ArchiveSizeBytes: ptrInt64(200), GapSegmentCount: 2, Integrity: "degraded"},
		{ID: "a", Title: "Concert Live", AdapterID: "other", AdapterName: "Other Adapter", State: "completed", ResourceType: "alpha", ResourceID: "stable-a", Tags: []string{"archive"}, StartedAt: base, CreatedAt: base.Add(24 * time.Hour), DurationSeconds: 20, ArchiveSizeBytes: ptrInt64(300), GapDurationSeconds: nil, Integrity: "unknown"},
		{ID: "c", Title: "Late Show", AdapterID: "third", AdapterName: "Third", State: "interrupted", ResourceType: "gamma", ResourceID: "stable-c", Tags: []string{"concert", "important"}, StartedAt: base.Add(48 * time.Hour), CreatedAt: base.Add(72 * time.Hour), DurationSeconds: 40, ArchiveSizeBytes: ptrInt64(50), GapCount: 1, Integrity: "failed"},
	}
}

func ptr(v float64) *float64 { return &v }

func ptrInt64(v int64) *int64 { return &v }

func TestParseQueryDatesAndLimits(t *testing.T) {
	values := url.Values{
		"started_after":  {"2024-01-02T12:00:00+00:00"},
		"started_before": {"2024-01-04T12:00:00Z"},
		"limit":          {"999"},
		"has_gaps":       {"false"},
	}
	q, err := ParseQuery(values)
	if err != nil {
		t.Fatal(err)
	}
	if q.Limit != maximumLimit || q.Sort != defaultSort {
		t.Fatalf("unexpected defaults/limit: limit=%d sort=%q", q.Limit, q.Sort)
	}
	if q.StartedAfter == nil || q.StartedAfter.Format(time.RFC3339) != "2024-01-02T12:00:00Z" {
		t.Fatalf("started_after not parsed/normalized: %v", q.StartedAfter)
	}
	if q.HasGaps == nil || *q.HasGaps {
		t.Fatalf("has_gaps not parsed: %v", q.HasGaps)
	}

	if q, err = ParseQuery(url.Values{}); err != nil || q.Limit != defaultLimit {
		t.Fatalf("default limit: q=%+v err=%v", q, err)
	}
	if _, err = ParseQuery(url.Values{"limit": {"-1"}}); err == nil {
		t.Fatal("negative limit was accepted")
	}
	if _, err = ParseQuery(url.Values{"started_after": {"yesterday"}}); err == nil {
		t.Fatal("non-RFC3339 date was accepted")
	}
	if _, err = ParseQuery(url.Values{"started_after": {"2024-03-01T00:00:00Z"}, "started_before": {"2024-02-01T00:00:00Z"}}); err == nil {
		t.Fatal("inverted date range was accepted")
	}
}

func TestPageSearchesAcrossFieldsCaseInsensitively(t *testing.T) {
	items := fixtureItems()
	tests := []struct {
		name string
		q    string
		want []string
	}{
		{"title and tags", "cOnCeRt", []string{"c", "z", "a"}},
		{"adapter id", "OWNCAST", []string{"z"}},
		{"adapter name", "other adapter", []string{"b", "a"}},
		{"resource stable id", "STABLE-C", []string{"c"}},
		{"tag", "important", []string{"c", "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Page(items, Query{Q: tt.q})
			if err != nil {
				t.Fatal(err)
			}
			if got := ids(result.Items); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got IDs %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPageAppliesEveryFilterBeforePagination(t *testing.T) {
	base := time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC)
	trueValue, falseValue := true, false
	tests := []struct {
		name string
		q    Query
		want []string
	}{
		{"state", Query{State: "stopped"}, []string{"b"}},
		{"adapter", Query{Adapter: "owncast"}, []string{"z"}},
		{"resource type", Query{ResourceType: "gamma"}, []string{"c"}},
		{"started after inclusive", Query{StartedAfter: &base}, []string{"b", "z", "c"}},
		{"started before inclusive", Query{StartedBefore: &base}, []string{"a", "b", "z"}},
		{"has gaps", Query{HasGaps: &trueValue}, []string{"b", "z", "c"}},
		{"no gaps", Query{HasGaps: &falseValue}, []string{"a"}},
		{"integrity", Query{Integrity: "degraded"}, []string{"b"}},
		{"tag exact folded", Query{Tag: "CONCERT"}, []string{"z", "c"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.q.Sort = "started_at"
			result, err := Page(fixtureItems(), tt.q)
			if err != nil {
				t.Fatal(err)
			}
			got := ids(result.Items)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			if result.Total != len(tt.want) {
				t.Fatalf("total=%d, want %d", result.Total, len(tt.want))
			}
		})
	}

	combined, err := Page(fixtureItems(), Query{State: "completed", ResourceType: "alpha", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if combined.Total != 2 || len(combined.Items) != 1 || combined.NextCursor == "" {
		t.Fatalf("filter should precede pagination: %+v", combined)
	}
}

func TestPageSupportsAllSortDirectionsAndIDTieBreak(t *testing.T) {
	tests := []struct {
		sort string
		want []string
	}{
		{"started_at", []string{"a", "b", "z", "c"}},
		{"-started_at", []string{"c", "b", "z", "a"}},
		{"created_at", []string{"z", "a", "b", "c"}},
		{"-created_at", []string{"c", "b", "a", "z"}},
		{"duration", []string{"z", "a", "b", "c"}},
		{"-duration", []string{"c", "b", "a", "z"}},
		{"size", []string{"c", "z", "b", "a"}},
		{"-size", []string{"a", "b", "z", "c"}},
	}
	for _, tt := range tests {
		t.Run(tt.sort, func(t *testing.T) {
			result, err := Page(fixtureItems(), Query{Sort: tt.sort})
			if err != nil {
				t.Fatal(err)
			}
			if got := ids(result.Items); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func archiveSizeItems() []Item {
	return []Item{
		{ID: "unknown-b", ArchiveSizeBytes: nil},
		{ID: "known-20", ArchiveSizeBytes: ptrInt64(20)},
		{ID: "unknown-a", ArchiveSizeBytes: nil},
		{ID: "known-0", ArchiveSizeBytes: ptrInt64(0)},
		{ID: "known-10", ArchiveSizeBytes: ptrInt64(10)},
	}
}

func TestSizeSortKeepsUnknownLastInBothDirections(t *testing.T) {
	tests := []struct {
		sort string
		want []string
	}{
		{"size", []string{"known-0", "known-10", "known-20", "unknown-a", "unknown-b"}},
		{"-size", []string{"known-20", "known-10", "known-0", "unknown-a", "unknown-b"}},
	}
	for _, test := range tests {
		t.Run(test.sort, func(t *testing.T) {
			result, err := Page(archiveSizeItems(), Query{Sort: test.sort})
			if err != nil {
				t.Fatal(err)
			}
			if got := ids(result.Items); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("got IDs %v, want %v", got, test.want)
			}
		})
	}
}

func TestSizeCursorDistinguishesKnownZeroFromUnknown(t *testing.T) {
	query, err := normalize(Query{Sort: "size", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := filterFingerprint(query)
	if err != nil {
		t.Fatal(err)
	}

	legacyJSON := `{"v":1,"k":"size","x":"0","i":"known-0","f":"` + fingerprint + `"}`
	legacyCursor := base64.RawURLEncoding.EncodeToString([]byte(legacyJSON))
	result, err := Page(archiveSizeItems(), Query{Sort: "size", Limit: 1, Cursor: legacyCursor})
	if err != nil {
		t.Fatalf("legacy known-size cursor rejected: %v", err)
	}
	if got := ids(result.Items); !reflect.DeepEqual(got, []string{"known-10"}) {
		t.Fatalf("legacy zero cursor was treated as unknown: got %v", got)
	}

	unknown := Item{ID: "unknown-a"}
	encoded, err := encodeCursor(unknown, "size", fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var cursor cursorData
	if err := json.Unmarshal(data, &cursor); err != nil {
		t.Fatal(err)
	}
	if cursor.Value != "0" || !cursor.Missing {
		t.Fatalf("unknown cursor must mark missing zero distinctly: %+v", cursor)
	}

	known, err := encodeCursor(Item{ID: "known-0", ArchiveSizeBytes: ptrInt64(0)}, "size", fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	knownData, err := base64.RawURLEncoding.DecodeString(known)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(knownData), `"m"`) {
		t.Fatalf("known-size cursor changed legacy representation: %s", knownData)
	}
}

func TestSizeCursorPaginationHasNoDuplicatesOrSkips(t *testing.T) {
	items := archiveSizeItems()
	for _, sortKey := range []string{"size", "-size"} {
		sortKey := sortKey
		full, err := Page(items, Query{Sort: sortKey, Limit: maximumLimit})
		if err != nil {
			t.Fatal(err)
		}
		want := ids(full.Items)
		for _, limit := range []int{1, 2} {
			limit := limit
			t.Run(sortKey+"/limit="+strconv.Itoa(limit), func(t *testing.T) {
				query := Query{Sort: sortKey, Limit: limit}
				var got []string
				for pageCount := 0; ; pageCount++ {
					if pageCount > len(items) {
						t.Fatal("cursor pagination did not terminate")
					}
					page, err := Page(items, query)
					if err != nil {
						t.Fatal(err)
					}
					got = append(got, ids(page.Items)...)
					if page.NextCursor == "" {
						break
					}
					query.Cursor = page.NextCursor
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("paged IDs %v, want %v", got, want)
				}
			})
		}
	}
}

func TestCursorPagesHaveNoDuplicatesOrSkips(t *testing.T) {
	items := fixtureItems()
	query := Query{Sort: "started_at", Limit: 2}
	var got []string
	for {
		result, err := Page(items, query)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, ids(result.Items)...)
		if result.NextCursor == "" {
			break
		}
		query.Cursor = result.NextCursor
		// The cursor represents ordering/filter state, not page size.
		query.Limit = 1
	}
	want := []string{"a", "b", "z", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paged IDs %v, want %v", got, want)
	}
}

func TestCursorRejectsFilterMismatchAndMalformedInput(t *testing.T) {
	first, err := Page(fixtureItems(), Query{Sort: "size", Limit: 1, Adapter: "other"})
	if err != nil {
		t.Fatal(err)
	}
	if first.NextCursor == "" {
		t.Fatal("expected continuation cursor")
	}
	if _, err := Page(fixtureItems(), Query{Sort: "size", Adapter: "third", Cursor: first.NextCursor}); err == nil {
		t.Fatal("cursor/filter mismatch was accepted")
	}
	if _, err := Page(fixtureItems(), Query{Sort: "-size", Adapter: "other", Cursor: first.NextCursor}); err == nil {
		t.Fatal("cursor/sort mismatch was accepted")
	}
	for _, malformed := range []string{"!", base64String(`{"v":1}`), strings.Repeat("a", maxCursorLen+1)} {
		if _, err := Page(fixtureItems(), Query{Cursor: malformed}); err == nil {
			t.Fatalf("malformed cursor %q was accepted", malformed)
		}
	}
}

func TestPageEmptySetAndLimitBounds(t *testing.T) {
	result, err := Page(nil, Query{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 0 || len(result.Items) != 0 || result.NextCursor != "" {
		t.Fatalf("unexpected empty result: %+v", result)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"items":[]`) {
		t.Fatalf("empty items must encode as an array: %s", encoded)
	}
	result, err = Page(fixtureItems(), Query{Limit: maximumLimit + 1})
	if err != nil || len(result.Items) != 4 {
		t.Fatalf("large limit should clamp: result=%+v err=%v", result, err)
	}
	if _, err := Page(fixtureItems(), Query{Limit: -1}); err == nil {
		t.Fatal("negative limit was accepted")
	}
	if _, err := Page([]Item{{ID: "duplicate"}, {ID: "duplicate"}}, Query{}); err == nil {
		t.Fatal("duplicate IDs were accepted")
	}
}

func TestParseQueryRejectsUnknownRepeatedAndOversizedFilters(t *testing.T) {
	tests := []struct {
		name   string
		values url.Values
	}{
		{"unknown", url.Values{"debug": {"true"}}},
		{"repeated q", url.Values{"q": {"first", "second"}}},
		{"oversized search", url.Values{"q": {strings.Repeat("x", maxSearchLen+1)}}},
		{"oversized adapter", url.Values{"adapter": {strings.Repeat("x", maxFilterLen+1)}}},
		{"control character", url.Values{"tag": {"safe\nunsafe"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseQuery(tt.values); err == nil {
				t.Fatal("invalid query was accepted")
			}
		})
	}
}

func TestPageRejectsOversizedDirectQuery(t *testing.T) {
	if _, err := Page(fixtureItems(), Query{Q: strings.Repeat("x", maxSearchLen+1)}); err == nil {
		t.Fatal("oversized direct query was accepted")
	}
}

func ids(items []Item) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.ID)
	}
	return out
}

// base64String produces a URL-safe cursor-shaped value for malformed-payload tests.
func base64String(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}
