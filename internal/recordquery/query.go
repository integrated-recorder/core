// Package recordquery provides deterministic filtering, sorting, and cursor
// pagination for prepared recording summaries. It has no storage or filesystem
// responsibilities.
package recordquery

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/preview"
	"unicode"
	"unicode/utf8"
)

const (
	defaultLimit = 50
	maximumLimit = 100
	maxCursorLen = 4096
	maxSearchLen = 128
	maxFilterLen = 128
	defaultSort  = "-started_at"
)

var errInvalidQuery = errors.New("invalid recording query")

// Item is a prepared recording read-model item. Fields are populated by the
// caller from canonical recording data and management projections.
type Item struct {
	ID                    string           `json:"id"`
	Title                 string           `json:"title"`
	AdapterID             string           `json:"adapter_id"`
	AdapterName           string           `json:"adapter_name"`
	State                 string           `json:"state"`
	ResourceType          string           `json:"resource_type,omitempty"`
	ResourceID            string           `json:"resource_id,omitempty"`
	Tags                  []string         `json:"tags"`
	StartedAt             time.Time        `json:"started_at"`
	CreatedAt             time.Time        `json:"created_at"`
	DurationSeconds       float64          `json:"duration_seconds"`
	ArchiveSizeBytes      *int64           `json:"archive_size_bytes"`
	MediaPayloadSizeBytes int64            `json:"media_payload_size_bytes"`
	ManifestSizeBytes     int64            `json:"manifest_size_bytes"`
	InitPayloadSizeBytes  int64            `json:"init_payload_size_bytes"`
	SegmentCount          int              `json:"segment_count"`
	InitSegmentCount      int              `json:"init_segment_count"`
	ManifestSnapshotCount int              `json:"manifest_snapshot_count"`
	GapCount              int              `json:"gap_count"`
	GapSegmentCount       int              `json:"gap_segment_count"`
	GapDurationSeconds    *float64         `json:"gap_duration_seconds"`
	Integrity             string           `json:"integrity"`
	Preview               *preview.Summary `json:"preview,omitempty"`
	StatisticsStatus      string           `json:"statistics_status"`
	UnavailableFields     []string         `json:"unavailable_fields"`
}

// Query contains the normalized recording-list filters. StartedAfter and
// StartedBefore are inclusive UTC boundaries. A zero Limit means defaultLimit;
// limits above maximumLimit are clamped, while negative limits are rejected.
type Query struct {
	Q             string
	State         string
	Adapter       string
	ResourceType  string
	StartedAfter  *time.Time
	StartedBefore *time.Time
	HasGaps       *bool
	Integrity     string
	Tag           string
	Sort          string
	Limit         int
	Cursor        string
}

// Result is one page of recording summaries. Total counts matching items
// before pagination.
type Result struct {
	Items      []Item `json:"items"`
	NextCursor string `json:"next_cursor"`
	Total      int    `json:"total"`
}

var validSorts = map[string]string{
	"started_at":  "started_at",
	"-started_at": "started_at",
	"created_at":  "created_at",
	"-created_at": "created_at",
	"duration":    "duration",
	"-duration":   "duration",
	"size":        "size",
	"-size":       "size",
}

// ParseQuery parses and validates the supported URL query parameters. Limits
// greater than 100 are clamped; a negative limit is rejected.
func ParseQuery(values url.Values) (Query, error) {
	allowed := map[string]struct{}{
		"q": {}, "state": {}, "adapter": {}, "resource_type": {},
		"started_after": {}, "started_before": {}, "has_gaps": {},
		"integrity": {}, "tag": {}, "sort": {}, "limit": {}, "cursor": {},
	}
	for key, entries := range values {
		if _, ok := allowed[key]; !ok || len(entries) > 1 {
			return Query{}, fmt.Errorf("%w: unsupported or repeated query parameter", errInvalidQuery)
		}
	}
	var q Query
	var err error
	if q.Q, err = one(values, "q"); err != nil {
		return Query{}, err
	}
	if q.State, err = one(values, "state"); err != nil {
		return Query{}, err
	}
	if q.Adapter, err = one(values, "adapter"); err != nil {
		return Query{}, err
	}
	if q.ResourceType, err = one(values, "resource_type"); err != nil {
		return Query{}, err
	}
	if q.Integrity, err = one(values, "integrity"); err != nil {
		return Query{}, err
	}
	if q.Tag, err = one(values, "tag"); err != nil {
		return Query{}, err
	}
	if q.Sort, err = one(values, "sort"); err != nil {
		return Query{}, err
	}
	if q.Cursor, err = one(values, "cursor"); err != nil {
		return Query{}, err
	}
	if q.Limit, err = parseLimit(values); err != nil {
		return Query{}, err
	}
	if q.StartedAfter, err = parseDate(values, "started_after"); err != nil {
		return Query{}, err
	}
	if q.StartedBefore, err = parseDate(values, "started_before"); err != nil {
		return Query{}, err
	}
	if raw, e := one(values, "has_gaps"); e != nil {
		return Query{}, e
	} else if raw != "" {
		v, e := strconv.ParseBool(raw)
		if e != nil {
			return Query{}, fmt.Errorf("%w: has_gaps must be true or false", errInvalidQuery)
		}
		q.HasGaps = &v
	}
	return normalize(q)
}

func one(values url.Values, key string) (string, error) {
	vs, ok := values[key]
	if !ok || len(vs) == 0 {
		return "", nil
	}
	if len(vs) != 1 {
		return "", fmt.Errorf("%w: %s must occur once", errInvalidQuery, key)
	}
	return strings.TrimSpace(vs[0]), nil
}

func parseLimit(values url.Values) (int, error) {
	raw, err := one(values, "limit")
	if err != nil {
		return 0, err
	}
	if raw == "" {
		return defaultLimit, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 0 {
		return 0, fmt.Errorf("%w: limit must be a non-negative integer", errInvalidQuery)
	}
	if limit == 0 {
		return defaultLimit, nil
	}
	if limit > maximumLimit {
		return maximumLimit, nil
	}
	return limit, nil
}

func parseDate(values url.Values, key string) (*time.Time, error) {
	raw, err := one(values, key)
	if err != nil || raw == "" {
		return nil, err
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %s must be RFC3339", errInvalidQuery, key)
	}
	t = t.UTC()
	return &t, nil
}

func normalize(q Query) (Query, error) {
	q.Q = strings.TrimSpace(q.Q)
	q.State = strings.TrimSpace(q.State)
	q.Adapter = strings.TrimSpace(q.Adapter)
	q.ResourceType = strings.TrimSpace(q.ResourceType)
	q.Integrity = strings.TrimSpace(q.Integrity)
	q.Tag = strings.TrimSpace(q.Tag)
	q.Cursor = strings.TrimSpace(q.Cursor)
	q.Sort = strings.TrimSpace(q.Sort)
	if q.Sort == "" {
		q.Sort = defaultSort
	}
	if _, ok := validSorts[q.Sort]; !ok {
		return Query{}, fmt.Errorf("%w: unsupported sort", errInvalidQuery)
	}
	if q.Limit < 0 {
		return Query{}, fmt.Errorf("%w: limit must be non-negative", errInvalidQuery)
	}
	if q.Limit == 0 {
		q.Limit = defaultLimit
	}
	if q.Limit > maximumLimit {
		q.Limit = maximumLimit
	}
	if len(q.Q) > maxSearchLen || !validFilter(q.Q) ||
		len(q.State) > maxFilterLen || !validFilter(q.State) ||
		len(q.Adapter) > maxFilterLen || !validFilter(q.Adapter) ||
		len(q.ResourceType) > maxFilterLen || !validFilter(q.ResourceType) ||
		len(q.Integrity) > maxFilterLen || !validFilter(q.Integrity) ||
		len(q.Tag) > maxFilterLen || !validFilter(q.Tag) {
		return Query{}, fmt.Errorf("%w: filter is invalid or too long", errInvalidQuery)
	}
	if q.StartedAfter != nil && q.StartedBefore != nil && q.StartedAfter.After(*q.StartedBefore) {
		return Query{}, fmt.Errorf("%w: started_after must not be after started_before", errInvalidQuery)
	}
	if len(q.Cursor) > maxCursorLen {
		return Query{}, fmt.Errorf("%w: cursor is too long", errInvalidQuery)
	}
	return q, nil
}

func validFilter(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// Page applies filters before sorting and pagination. The input slice is never
// modified. Cursor fingerprints intentionally exclude limit so clients may
// change page size while continuing the same filtered query.
func Page(items []Item, query Query) (Result, error) {
	q, err := normalize(query)
	if err != nil {
		return Result{}, err
	}
	fingerprint, err := filterFingerprint(q)
	if err != nil {
		return Result{}, err
	}
	var cursor *cursorData
	if q.Cursor != "" {
		cursor, err = decodeCursor(q.Cursor)
		if err != nil {
			return Result{}, err
		}
		if cursor.Version != 1 || cursor.Fingerprint != fingerprint || cursor.Key != validSorts[q.Sort] {
			return Result{}, fmt.Errorf("%w: cursor does not match query", errInvalidQuery)
		}
	}

	filtered := make([]Item, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item.ID == "" {
			return Result{}, fmt.Errorf("%w: item ID is required", errInvalidQuery)
		}
		if _, ok := seen[item.ID]; ok {
			return Result{}, fmt.Errorf("%w: duplicate item ID", errInvalidQuery)
		}
		seen[item.ID] = struct{}{}
		if err := validateItem(item); err != nil {
			return Result{}, err
		}
		if matches(item, q) {
			filtered = append(filtered, item)
		}
	}
	total := len(filtered)
	sortItems(filtered, q.Sort)

	start := 0
	if cursor != nil {
		anchor, err := cursorItem(*cursor, q.Sort)
		if err != nil {
			return Result{}, err
		}
		for start < len(filtered) && compare(filtered[start], anchor, q.Sort) <= 0 {
			start++
		}
	}
	end := start + q.Limit
	if end > len(filtered) {
		end = len(filtered)
	}
	pageItems := append(make([]Item, 0, end-start), filtered[start:end]...)
	next := ""
	if end < len(filtered) && len(pageItems) > 0 {
		last := pageItems[len(pageItems)-1]
		next, err = encodeCursor(last, q.Sort, fingerprint)
		if err != nil {
			return Result{}, err
		}
	}
	return Result{Items: pageItems, NextCursor: next, Total: total}, nil
}

func validateItem(item Item) error {
	if math.IsNaN(item.DurationSeconds) || math.IsInf(item.DurationSeconds, 0) || item.DurationSeconds < 0 {
		return fmt.Errorf("%w: invalid duration summary", errInvalidQuery)
	}
	if item.ArchiveSizeBytes != nil && *item.ArchiveSizeBytes < 0 || item.MediaPayloadSizeBytes < 0 || item.ManifestSizeBytes < 0 || item.InitPayloadSizeBytes < 0 || item.SegmentCount < 0 || item.InitSegmentCount < 0 || item.ManifestSnapshotCount < 0 || item.GapCount < 0 || item.GapSegmentCount < 0 {
		return fmt.Errorf("%w: invalid statistics summary", errInvalidQuery)
	}
	if item.GapDurationSeconds != nil && (math.IsNaN(*item.GapDurationSeconds) || math.IsInf(*item.GapDurationSeconds, 0) || *item.GapDurationSeconds < 0) {
		return fmt.Errorf("%w: invalid gap duration summary", errInvalidQuery)
	}
	return nil
}

func matches(item Item, q Query) bool {
	if q.Q != "" {
		needle := strings.ToLower(q.Q)
		found := containsFold(item.Title, needle) || containsFold(item.AdapterID, needle) || containsFold(item.AdapterName, needle) || containsFold(item.ResourceID, needle)
		if !found {
			for _, tag := range item.Tags {
				if containsFold(tag, needle) {
					found = true
					break
				}
			}
		}
		if !found {
			return false
		}
	}
	if q.State != "" && item.State != q.State {
		return false
	}
	if q.Adapter != "" && item.AdapterID != q.Adapter {
		return false
	}
	if q.ResourceType != "" && item.ResourceType != q.ResourceType {
		return false
	}
	if q.StartedAfter != nil && item.StartedAt.Before(*q.StartedAfter) {
		return false
	}
	if q.StartedBefore != nil && item.StartedAt.After(*q.StartedBefore) {
		return false
	}
	if q.HasGaps != nil {
		hasGaps := item.GapCount > 0 || item.GapSegmentCount > 0
		if hasGaps != *q.HasGaps {
			return false
		}
	}
	if q.Integrity != "" && item.Integrity != q.Integrity {
		return false
	}
	if q.Tag != "" && !hasTag(item.Tags, q.Tag) {
		return false
	}
	return true
}

func containsFold(value, lowerNeedle string) bool {
	return strings.Contains(strings.ToLower(value), lowerNeedle)
}

func hasTag(tags []string, wanted string) bool {
	for _, tag := range tags {
		if strings.EqualFold(tag, wanted) {
			return true
		}
	}
	return false
}

func sortItems(items []Item, sortKey string) {
	// Sorting is implemented locally to keep the package's behavior explicit
	// and avoid modifying the caller's slice.
	sort.Slice(items, func(i, j int) bool { return compare(items[i], items[j], sortKey) < 0 })
}

func compare(a, b Item, sortKey string) int {
	desc := strings.HasPrefix(sortKey, "-")
	key := strings.TrimPrefix(sortKey, "-")
	primary := 0
	switch key {
	case "started_at":
		primary = compareTime(a.StartedAt, b.StartedAt)
	case "created_at":
		primary = compareTime(a.CreatedAt, b.CreatedAt)
	case "duration":
		primary = compareFloat(a.DurationSeconds, b.DurationSeconds)
	case "size":
		primary = compareInt64(archiveSizeValue(a.ArchiveSizeBytes), archiveSizeValue(b.ArchiveSizeBytes))
	}
	if desc {
		primary = -primary
	}
	if primary != 0 {
		return primary
	}
	return strings.Compare(a.ID, b.ID)
}

func archiveSizeValue(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func compareTime(a, b time.Time) int {
	if a.Before(b) {
		return -1
	}
	if a.After(b) {
		return 1
	}
	return 0
}

func compareFloat(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareInt64(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

type cursorData struct {
	Version     int    `json:"v"`
	Key         string `json:"k"`
	Value       string `json:"x"`
	ID          string `json:"i"`
	Fingerprint string `json:"f"`
}

func encodeCursor(item Item, sortKey, fingerprint string) (string, error) {
	key := validSorts[sortKey]
	value, err := sortValue(item, key)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(cursorData{Version: 1, Key: key, Value: value, ID: item.ID, Fingerprint: fingerprint})
	if err != nil {
		return "", fmt.Errorf("encode cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeCursor(raw string) (*cursorData, error) {
	if len(raw) > maxCursorLen {
		return nil, fmt.Errorf("%w: cursor is too long", errInvalidQuery)
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(data) > maxCursorLen {
		return nil, fmt.Errorf("%w: malformed cursor", errInvalidQuery)
	}
	var cursor cursorData
	if err := json.Unmarshal(data, &cursor); err != nil || cursor.ID == "" || cursor.Key == "" || cursor.Value == "" || cursor.Fingerprint == "" {
		return nil, fmt.Errorf("%w: malformed cursor", errInvalidQuery)
	}
	return &cursor, nil
}

func cursorItem(cursor cursorData, sortKey string) (Item, error) {
	item := Item{ID: cursor.ID}
	key := validSorts[sortKey]
	switch key {
	case "started_at", "created_at":
		value, err := time.Parse(time.RFC3339Nano, cursor.Value)
		if err != nil {
			return Item{}, fmt.Errorf("%w: malformed cursor value", errInvalidQuery)
		}
		if key == "started_at" {
			item.StartedAt = value
		} else {
			item.CreatedAt = value
		}
	case "duration":
		value, err := strconv.ParseFloat(cursor.Value, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			return Item{}, fmt.Errorf("%w: malformed cursor value", errInvalidQuery)
		}
		item.DurationSeconds = value
	case "size":
		value, err := strconv.ParseInt(cursor.Value, 10, 64)
		if err != nil {
			return Item{}, fmt.Errorf("%w: malformed cursor value", errInvalidQuery)
		}
		item.ArchiveSizeBytes = &value
	default:
		return Item{}, fmt.Errorf("%w: malformed cursor key", errInvalidQuery)
	}
	return item, nil
}

func sortValue(item Item, key string) (string, error) {
	switch key {
	case "started_at":
		return item.StartedAt.UTC().Format(time.RFC3339Nano), nil
	case "created_at":
		return item.CreatedAt.UTC().Format(time.RFC3339Nano), nil
	case "duration":
		return strconv.FormatFloat(item.DurationSeconds, 'g', -1, 64), nil
	case "size":
		return strconv.FormatInt(archiveSizeValue(item.ArchiveSizeBytes), 10), nil
	default:
		return "", fmt.Errorf("%w: unsupported cursor key", errInvalidQuery)
	}
}

func filterFingerprint(q Query) (string, error) {
	type filter struct {
		Q             string `json:"q"`
		State         string `json:"state"`
		Adapter       string `json:"adapter"`
		ResourceType  string `json:"resource_type"`
		StartedAfter  string `json:"started_after"`
		StartedBefore string `json:"started_before"`
		HasGaps       *bool  `json:"has_gaps"`
		Integrity     string `json:"integrity"`
		Tag           string `json:"tag"`
		Sort          string `json:"sort"`
	}
	f := filter{Q: q.Q, State: q.State, Adapter: q.Adapter, ResourceType: q.ResourceType, HasGaps: q.HasGaps, Integrity: q.Integrity, Tag: q.Tag, Sort: q.Sort}
	if q.StartedAfter != nil {
		f.StartedAfter = q.StartedAfter.UTC().Format(time.RFC3339Nano)
	}
	if q.StartedBefore != nil {
		f.StartedBefore = q.StartedBefore.UTC().Format(time.RFC3339Nano)
	}
	data, err := json.Marshal(f)
	if err != nil {
		return "", fmt.Errorf("fingerprint filters: %w", err)
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}
