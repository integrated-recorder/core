package storageprocess

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/storageproto"
)

func TestListPageWithFallbackPreservesEnumeration(t *testing.T) {
	items := make([]storageproto.ObjectEntry, 708)
	for i := range items {
		items[i] = storageproto.ObjectEntry{Key: fmt.Sprintf("recordings/%032x/archive/v2/manifests/%020d-%s.json", 1, i, strings.Repeat("x", 48)), Size: int64(i)}
	}
	var requestedLimits []int
	request := func(_ context.Context, prefix, cursor string, limit int) (storageproto.ListPage, error) {
		requestedLimits = append(requestedLimits, limit)
		if limit > 250 {
			return storageproto.ListPage{}, storageproto.ErrUnframedResponse
		}
		start := 0
		for start < len(items) && items[start].Key <= cursor {
			start++
		}
		end := min(start+limit, len(items))
		page := storageproto.ListPage{Items: append([]storageproto.ObjectEntry{}, items[start:end]...)}
		if end < len(items) && len(page.Items) > 0 {
			page.NextCursor = page.Items[len(page.Items)-1].Key
		}
		for _, item := range page.Items {
			if !strings.HasPrefix(item.Key, prefix) {
				t.Fatalf("test provider returned key outside prefix: %q", item.Key)
			}
		}
		return page, nil
	}

	var got []string
	cursor := ""
	limit := 1000
	for len(got) < len(items) {
		page, usedLimit, err := listPageWithFallback(context.Background(), "recordings/", cursor, limit, request)
		if err != nil {
			t.Fatalf("list page at cursor %q: %v", cursor, err)
		}
		if usedLimit < limit {
			limit = usedLimit
		}
		if len(page.Items) == 0 {
			t.Fatal("list fallback returned an empty page before enumeration completed")
		}
		for _, item := range page.Items {
			got = append(got, item.Key)
		}
		cursor = page.NextCursor
		if cursor == "" && len(got) != len(items) {
			t.Fatalf("listing ended after %d objects, want %d", len(got), len(items))
		}
	}
	want := make([]string, len(items))
	for i := range items {
		want[i] = items[i].Key
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("fallback enumeration differs: got=%d want=%d", len(got), len(want))
	}
	if !reflect.DeepEqual(requestedLimits[:3], []int{1000, 500, 250}) {
		t.Fatalf("initial fallback limits=%v, want [1000 500 250]", requestedLimits[:3])
	}
	if requestedLimits[3] != 250 {
		t.Fatalf("subsequent request limit=%d, want cached 250", requestedLimits[3])
	}

	calls := 0
	_, _, err := listPageWithFallback(context.Background(), "recordings/", "", 1000, func(context.Context, string, string, int) (storageproto.ListPage, error) {
		calls++
		return storageproto.ListPage{}, errors.New("provider unavailable")
	})
	if err == nil || calls != 1 {
		t.Fatalf("non-frame error was retried: calls=%d err=%v", calls, err)
	}
}
