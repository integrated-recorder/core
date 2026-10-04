//go:build linux || darwin

package storagelocal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/buildinfo"
	"github.com/integrated-recorder/core/internal/storageproto"
)

func TestDescriptorAndRequiredRootConfiguration(t *testing.T) {
	provider := New()
	descriptor := provider.Descriptor()
	if err := storageproto.ValidateDescriptor(descriptor); err != nil {
		t.Fatalf("descriptor is invalid: %v", err)
	}
	if descriptor.ID != "local" || descriptor.Name != "Local Storage" || descriptor.Version != buildinfo.Current().Version || descriptor.ProtocolVersion != storageproto.Version {
		t.Fatalf("unexpected descriptor identity: %#v", descriptor)
	}
	if len(descriptor.ConfigurationSchema.Fields) != 1 {
		t.Fatalf("expected one config field, got %#v", descriptor.ConfigurationSchema.Fields)
	}
	field := descriptor.ConfigurationSchema.Fields[0]
	if field.Key != "root" || field.Control != "text" || !field.Required {
		t.Fatalf("root config field is not required text: %#v", field)
	}

	root := filepath.Join(t.TempDir(), "recordings")
	if err := provider.Configure(context.Background(), rootConfig(t, root)); err != nil {
		t.Fatalf("configure root: %v", err)
	}
	if err := provider.Probe(context.Background()); err != nil {
		t.Fatalf("probe did not safely create/open the configured root: %v", err)
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("probe root is not a directory: info=%v err=%v", info, err)
	}
	for _, config := range []storageproto.Config{
		{},
		{Values: map[string]json.RawMessage{"root": json.RawMessage(`"relative"`)}},
		{Values: map[string]json.RawMessage{"root": json.RawMessage(`"/tmp/archive"`), "extra": json.RawMessage(`true`)}},
		{Secrets: map[string]string{"root": "/tmp/archive"}},
	} {
		if err := provider.Configure(context.Background(), config); err == nil {
			t.Fatalf("invalid configuration accepted: %#v", config)
		}
	}
}

func TestLegacyRecordingAndPrivateKeyMapping(t *testing.T) {
	provider, root := configuredProvider(t)
	legacy := []byte("legacy archive bytes stay in place")
	if _, err := provider.Put(context.Background(), "recordings/recording-42/tracks/main/00000001.m4s", bytes.NewReader(legacy), int64(len(legacy))); err != nil {
		t.Fatalf("put recording object: %v", err)
	}
	physicalLegacy := filepath.Join(root, "recording-42", "tracks", "main", "00000001.m4s")
	if got, err := os.ReadFile(physicalLegacy); err != nil || !bytes.Equal(got, legacy) {
		t.Fatalf("recording key did not map to the legacy path: bytes=%q err=%v", got, err)
	}

	markerKey := "recording-deletions/recording-42.json"
	marker := []byte("marker")
	if _, err := provider.Put(context.Background(), markerKey, bytes.NewReader(marker), int64(len(marker))); err != nil {
		t.Fatalf("put non-recording object: %v", err)
	}
	physicalMarker := filepath.Join(root, privateDirectory, privateObjects, "recording-deletions", "recording-42.json")
	if got, err := os.ReadFile(physicalMarker); err != nil || !bytes.Equal(got, marker) {
		t.Fatalf("non-recording key did not map to provider-private path: bytes=%q err=%v", got, err)
	}

	// The private component cannot be expressed in a valid logical key, so a
	// public recordings suffix with the same printable shape remains distinct.
	publicCollisionKey := "recordings/.ir-storage-local/other/collision"
	privateCollisionKey := "collision"
	for _, key := range []string{publicCollisionKey, privateCollisionKey} {
		payload := []byte(key)
		if _, err := provider.Put(context.Background(), key, bytes.NewReader(payload), int64(len(payload))); err != nil {
			t.Fatalf("put collision test key %s: %v", key, err)
		}
	}
	publicCollisionPath := filepath.Join(root, ".ir-storage-local", "other", "collision")
	privateCollisionPath := filepath.Join(root, privateDirectory, privateObjects, "collision")
	if bytes, err := os.ReadFile(publicCollisionPath); err != nil || string(bytes) != publicCollisionKey {
		t.Fatalf("public collision path bytes=%q err=%v", bytes, err)
	}
	if bytes, err := os.ReadFile(privateCollisionPath); err != nil || string(bytes) != privateCollisionKey {
		t.Fatalf("private collision path bytes=%q err=%v", bytes, err)
	}
	page, err := provider.List(context.Background(), "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	keys := entryKeys(page.Items)
	for _, expected := range []string{publicCollisionKey, privateCollisionKey} {
		if !contains(keys, expected) {
			t.Errorf("injective listing omitted %q from %q", expected, keys)
		}
	}
}

func TestPutOpenRangeStatAndAtomicReplace(t *testing.T) {
	provider, _ := configuredProvider(t)
	key := "recordings/recording-a/archive.bin"
	first := make([]byte, 512<<10)
	for index := range first {
		first[index] = byte((index*37 + 19) % 251)
	}
	wantFirst := objectInfo(first)
	gotFirst, err := provider.Put(context.Background(), key, bytes.NewReader(first), int64(len(first)))
	if err != nil || gotFirst != wantFirst {
		t.Fatalf("put info=%#v err=%v, want %#v", gotFirst, err, wantFirst)
	}
	stat, err := provider.Stat(context.Background(), key)
	if err != nil || stat != wantFirst {
		t.Fatalf("stat info=%#v err=%v, want %#v", stat, err, wantFirst)
	}
	opened, openedInfo, err := provider.Open(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	readBack, readErr := io.ReadAll(opened)
	closeErr := opened.Close()
	if readErr != nil || closeErr != nil || openedInfo != wantFirst || !bytes.Equal(readBack, first) {
		t.Fatalf("full read mismatch: info=%#v read=%v close=%v", openedInfo, readErr, closeErr)
	}

	const offset, length = int64(197), int64(6431)
	ranged, rangeInfo, err := provider.OpenRange(context.Background(), key, offset, length)
	if err != nil {
		t.Fatal(err)
	}
	rangeBytes, readErr := io.ReadAll(ranged)
	closeErr = ranged.Close()
	if readErr != nil || closeErr != nil || rangeInfo != wantFirst || !bytes.Equal(rangeBytes, first[offset:offset+length]) {
		t.Fatalf("range mismatch: info=%#v read=%v close=%v", rangeInfo, readErr, closeErr)
	}
	if reader, _, err := provider.OpenRange(context.Background(), key, int64(len(first)), 1); reader != nil || !errors.Is(err, storageproto.ErrInvalidRange) {
		if reader != nil {
			_ = reader.Close()
		}
		t.Fatalf("invalid range returned reader=%v err=%v", reader, err)
	}

	second := []byte("complete replacement")
	wantSecond := objectInfo(second)
	gotSecond, err := provider.Put(context.Background(), key, bytes.NewReader(second), int64(len(second)))
	if err != nil || gotSecond != wantSecond {
		t.Fatalf("replace info=%#v err=%v, want %#v", gotSecond, err, wantSecond)
	}
	ranged, _, err = provider.Open(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	readBack, readErr = io.ReadAll(ranged)
	_ = ranged.Close()
	if readErr != nil || !bytes.Equal(readBack, second) {
		t.Fatalf("replacement bytes=%q err=%v", readBack, readErr)
	}
}

func TestDigestCacheServesRepeatedStatAndOpen(t *testing.T) {
	provider, _ := configuredProvider(t)
	key := "recordings/cache/repeated.bin"
	data := bytes.Repeat([]byte("digest-cache"), 64<<10)
	want := objectInfo(data)
	if got, err := provider.Put(context.Background(), key, bytes.NewReader(data), int64(len(data))); err != nil || got != want {
		t.Fatalf("put info=%#v err=%v, want %#v", got, err, want)
	}
	if got, err := provider.Stat(context.Background(), key); err != nil || got != want {
		t.Fatalf("stat info=%#v err=%v, want %#v", got, err, want)
	}
	reader, got, err := provider.Open(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	read, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || got != want || !bytes.Equal(read, data) {
		t.Fatalf("open info=%#v read=%v close=%v", got, readErr, closeErr)
	}
	if hits := provider.digestCacheHitCount(); hits != 2 {
		t.Fatalf("unchanged Stat/Open should reuse digest; hits=%d want 2", hits)
	}
}

func TestDigestCacheReplacementInvalidatesPriorDigest(t *testing.T) {
	provider, _ := configuredProvider(t)
	key := "recordings/cache/replace.bin"
	oldBytes := []byte("old object bytes")
	newBytes := []byte("new object bytes")
	if _, err := provider.Put(context.Background(), key, bytes.NewReader(oldBytes), int64(len(oldBytes))); err != nil {
		t.Fatal(err)
	}
	if got, err := provider.Stat(context.Background(), key); err != nil || got != objectInfo(oldBytes) {
		t.Fatalf("old stat=%#v err=%v", got, err)
	}
	if got, err := provider.Put(context.Background(), key, bytes.NewReader(newBytes), int64(len(newBytes))); err != nil || got != objectInfo(newBytes) {
		t.Fatalf("replacement put=%#v err=%v", got, err)
	}
	if got, err := provider.Stat(context.Background(), key); err != nil || got != objectInfo(newBytes) {
		t.Fatalf("replacement stat=%#v err=%v", got, err)
	}
	if hits := provider.digestCacheHitCount(); hits != 2 {
		t.Fatalf("replacement digest should be cached after publication; hits=%d want 2", hits)
	}
}

func TestDigestCacheDetectsSameSizeMutationWithRestoredMtime(t *testing.T) {
	provider, root := configuredProvider(t)
	key := "recordings/cache/mutated.bin"
	original := bytes.Repeat([]byte{'a'}, 64<<10)
	changed := bytes.Repeat([]byte{'b'}, len(original))
	if _, err := provider.Put(context.Background(), key, bytes.NewReader(original), int64(len(original))); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Stat(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	hitsBefore := provider.digestCacheHitCount()
	// Allow filesystems with second-resolution change times to advance before
	// mutating. The restored mtime must not hide the ctime change from the cache.
	time.Sleep(1100 * time.Millisecond)
	path := filepath.Join(root, "cache", "mutated.bin")
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write(changed); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(path, stat.ModTime(), stat.ModTime()); err != nil {
		t.Fatal(err)
	}
	got, err := provider.Stat(context.Background(), key)
	if err != nil || got != objectInfo(changed) {
		t.Fatalf("mutated stat=%#v err=%v, want digest %s", got, err, objectInfo(changed).SHA256)
	}
	if _, reliable := versionOf(stat); reliable {
		if hits := provider.digestCacheHitCount(); hits != hitsBefore {
			t.Fatalf("same-size mutation with restored mtime unexpectedly hit cache: before=%d after=%d", hitsBefore, hits)
		}
		if _, err = provider.Stat(context.Background(), key); err != nil {
			t.Fatal(err)
		}
		if hits := provider.digestCacheHitCount(); hits != hitsBefore+1 {
			t.Fatalf("updated digest was not cached after rehash: before=%d after=%d", hitsBefore, hits)
		}
	} else {
		before := provider.digestCacheHitCount()
		if _, err = provider.Stat(context.Background(), key); err != nil {
			t.Fatal(err)
		}
		if hits := provider.digestCacheHitCount(); hits != before {
			t.Fatalf("platform without stable ctime/identity reused cache: before=%d after=%d", before, hits)
		}
	}
}

func TestDigestCacheInvalidationDeleteAndConfigure(t *testing.T) {
	provider, _ := configuredProvider(t)
	key := "recordings/cache/delete.bin"
	data := []byte("to be deleted")
	if _, err := provider.Put(context.Background(), key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if err := provider.Delete(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if len(provider.digests) != 0 || provider.digestLRU.Len() != 0 {
		t.Fatal("Delete left a digest cache entry")
	}

	if _, err := provider.Put(context.Background(), key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if len(provider.digests) == 0 {
		t.Fatal("Put did not cache the published object digest")
	}
	newRoot := filepath.Join(t.TempDir(), "other-root")
	if err := provider.Configure(context.Background(), rootConfig(t, newRoot)); err != nil {
		t.Fatal(err)
	}
	if len(provider.digests) != 0 || provider.digestLRU.Len() != 0 {
		t.Fatal("Configure root change left digest cache entries")
	}
}

func TestDigestCacheIsBoundedAndEvictsLeastRecentlyUsed(t *testing.T) {
	provider, _ := configuredProvider(t)
	version := objectVersion{device: 1, inode: 1, size: 1}
	for index := 0; index < maxDigestCacheEntries; index++ {
		provider.storeDigest(fmt.Sprintf("key-%04d", index), provider.rootEpoch, version, "digest")
	}
	if _, ok := provider.cachedDigest("key-0000", provider.rootEpoch, version); !ok {
		t.Fatal("oldest inserted entry was absent before eviction")
	}
	provider.storeDigest("newest", provider.rootEpoch, version, "digest")
	if len(provider.digests) != maxDigestCacheEntries || provider.digestLRU.Len() != maxDigestCacheEntries {
		t.Fatalf("digest cache exceeded limit: map=%d list=%d limit=%d", len(provider.digests), provider.digestLRU.Len(), maxDigestCacheEntries)
	}
	if _, ok := provider.cachedDigest("key-0001", provider.rootEpoch, version); ok {
		t.Fatal("least recently used entry was not evicted")
	}
	if _, ok := provider.cachedDigest("key-0000", provider.rootEpoch, version); !ok {
		t.Fatal("recently used entry was evicted")
	}
}

func TestNewProviderStartsWithColdDigestCache(t *testing.T) {
	first, root := configuredProvider(t)
	key := "recordings/cache/restart.bin"
	data := bytes.Repeat([]byte("restart"), 4096)
	want := objectInfo(data)
	if _, err := first.Put(context.Background(), key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}

	second := New()
	if err := second.Configure(context.Background(), rootConfig(t, root)); err != nil {
		t.Fatal(err)
	}
	if got, err := second.Stat(context.Background(), key); err != nil || got != want {
		t.Fatalf("cold stat=%#v err=%v, want %#v", got, err, want)
	}
	if hits := second.digestCacheHitCount(); hits != 0 {
		t.Fatalf("new provider should hash on first access, cache hits=%d", hits)
	}
	if got, err := second.Stat(context.Background(), key); err != nil || got != want {
		t.Fatalf("warm stat=%#v err=%v, want %#v", got, err, want)
	}
	if hits := second.digestCacheHitCount(); hits != 1 {
		t.Fatalf("new provider did not cache digest after first hash: hits=%d want 1", hits)
	}
}

func TestTraversalSymlinkAndHardlinkRejection(t *testing.T) {
	provider, root := configuredProvider(t)
	for _, key := range []string{"", "/etc/passwd", "../outside", "recordings//x", "recordings/./x", "recordings/a/../x", `recordings\outside`, "recordings/a\x00b", "recordings/a\nb"} {
		if _, err := provider.Stat(context.Background(), key); !errors.Is(err, storageproto.ErrInvalidKey) {
			t.Errorf("unsafe key %q returned %v", key, err)
		}
	}

	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "file-link")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := provider.Open(context.Background(), "recordings/file-link"); err == nil {
		t.Fatal("object symlink was opened")
	}
	if _, err := provider.Put(context.Background(), "recordings/file-link", bytes.NewReader([]byte("replace")), 7); err == nil {
		t.Fatal("object symlink was replaced")
	}
	outsideDirectory := t.TempDir()
	if err := os.Symlink(outsideDirectory, filepath.Join(root, "directory-link")); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Stat(context.Background(), "recordings/directory-link/sentinel"); err == nil {
		t.Fatal("symlink directory was traversed")
	}
	page, err := provider.List(context.Background(), "", "", 100)
	if err != nil {
		t.Fatalf("listing unsafe entries failed: %v", err)
	}
	for _, item := range page.Items {
		if item.Key == "recordings/file-link" || strings.HasPrefix(item.Key, "recordings/directory-link/") {
			t.Errorf("symlink appeared in object listing: %q", item.Key)
		}
	}
	if got, err := os.ReadFile(outside); err != nil || string(got) != "untouched" {
		t.Fatalf("outside symlink target changed: %q %v", got, err)
	}

	hardlinked := filepath.Join(root, "hardlinked")
	if err := os.WriteFile(hardlinked, []byte("shared"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(hardlinked, filepath.Join(root, "hardlink-alias")); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Stat(context.Background(), "recordings/hardlinked"); err == nil {
		t.Fatal("hardlinked object was accepted")
	}
	if _, err := provider.Put(context.Background(), "recordings/hardlinked", bytes.NewReader([]byte("new")), 3); err == nil {
		t.Fatal("hardlinked object was replaced")
	}
	if err := provider.Delete(context.Background(), "recordings/hardlinked"); err == nil {
		t.Fatal("hardlinked object was deleted")
	}
	if got, err := os.ReadFile(filepath.Join(root, "hardlink-alias")); err != nil || string(got) != "shared" {
		t.Fatalf("hardlink alias changed: %q %v", got, err)
	}
}

func TestListRoundTripsLogicalKeysWithBoundedPagination(t *testing.T) {
	provider, _ := configuredProvider(t)
	want := []string{
		"_integrated-recorder/system-probes/one",
		"conformance/session/item",
		"recording-deletions/recording-a.json",
		"recordings/recording-a/manifest.json",
		"recordings/recording-b/manifest.json",
	}
	for _, key := range want {
		payload := []byte("payload:" + key)
		if _, err := provider.Put(context.Background(), key, bytes.NewReader(payload), int64(len(payload))); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	var got []string
	cursor := ""
	for pageIndex := 0; ; pageIndex++ {
		if pageIndex > 10 {
			t.Fatal("pagination did not terminate")
		}
		page, err := provider.List(context.Background(), "", cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Items) > 2 {
			t.Fatalf("page exceeded requested bound: %d", len(page.Items))
		}
		for _, item := range page.Items {
			if item.SHA256 != "" || item.Size <= 0 {
				t.Errorf("unexpected listing metadata: %#v", item)
			}
			got = append(got, item.Key)
		}
		if page.NextCursor == "" {
			break
		}
		if len(page.Items) == 0 || page.NextCursor != page.Items[len(page.Items)-1].Key {
			t.Fatalf("invalid continuation cursor: %#v", page)
		}
		cursor = page.NextCursor
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("listed keys=%q, want %q", got, want)
	}

	page, err := provider.List(context.Background(), "recordings/", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if keys := entryKeys(page.Items); !reflect.DeepEqual(keys, []string{"recordings/recording-a/manifest.json", "recordings/recording-b/manifest.json"}) {
		t.Fatalf("recording prefix listed %q", keys)
	}
	page, err = provider.List(context.Background(), "recording-deletions/", "", 10)
	if err != nil || !reflect.DeepEqual(entryKeys(page.Items), []string{"recording-deletions/recording-a.json"}) {
		t.Fatalf("private-prefix list=%q err=%v", entryKeys(page.Items), err)
	}
}

func TestListTraversalBoundsAndCancellation(t *testing.T) {
	provider, _ := configuredProvider(t)
	for index := 0; index < 8; index++ {
		key := fmt.Sprintf("recordings/list-bound/item-%02d", index)
		payload := []byte("item")
		if _, err := provider.Put(context.Background(), key, bytes.NewReader(payload), int64(len(payload))); err != nil {
			t.Fatal(err)
		}
	}

	_, err := provider.listWithLimits(context.Background(), "", "", 10, listLimits{maxEntries: 2, maxDepth: maxListTraversalDepth})
	if !errors.Is(err, errListLimit) {
		t.Fatalf("total-entry ceiling returned %v", err)
	}
	_, err = provider.listWithLimits(context.Background(), "", "", 10, listLimits{maxEntries: 100, maxDepth: 1})
	if !errors.Is(err, errListLimit) {
		t.Fatalf("depth ceiling returned %v", err)
	}

	baseContext, cancel := context.WithCancel(context.Background())
	cancelingContext := &cancelAfterErrContext{Context: baseContext, cancel: cancel, cancelAt: 6}
	_, err = provider.List(cancelingContext, "", "", 10)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-list cancellation returned %v after %d checks", err, cancelingContext.checks)
	}
	if cancelingContext.checks < cancelingContext.cancelAt {
		t.Fatalf("listing did not reach the cancellation point: %d checks", cancelingContext.checks)
	}
}

func TestDeleteMissingShortExtraPayloadAndCancellation(t *testing.T) {
	provider, root := configuredProvider(t)
	key := "recordings/atomic/object"
	original := []byte("old complete contents")
	if _, err := provider.Put(context.Background(), key, bytes.NewReader(original), int64(len(original))); err != nil {
		t.Fatal(err)
	}
	for _, attempt := range []struct {
		name   string
		key    string
		reader io.Reader
		size   int64
	}{
		{name: "short replace", key: key, reader: bytes.NewReader([]byte("short")), size: 10},
		{name: "extra replace", key: key, reader: bytes.NewReader([]byte("extra bytes")), size: 5},
		{name: "short new object", key: "recordings/atomic/new", reader: bytes.NewReader([]byte("x")), size: 3},
	} {
		if _, err := provider.Put(context.Background(), attempt.key, attempt.reader, attempt.size); err == nil {
			t.Errorf("%s was accepted", attempt.name)
		}
	}
	cancelDuringStream, cancelStream := context.WithCancel(context.Background())
	cancelReader := &cancelAfterRead{cancel: cancelStream, bytes: []byte("partial")}
	if _, err := provider.Put(cancelDuringStream, key, cancelReader, int64(len(original))); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled streaming put returned %v", err)
	}
	cancelStream()
	opened, _, err := provider.Open(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	stillOriginal, readErr := io.ReadAll(opened)
	_ = opened.Close()
	if readErr != nil || !bytes.Equal(stillOriginal, original) {
		t.Fatalf("failed put changed old object to %q: %v", stillOriginal, readErr)
	}
	if _, err := provider.Stat(context.Background(), "recordings/atomic/new"); !errors.Is(err, storageproto.ErrNotFound) {
		t.Fatalf("failed new put returned %v", err)
	}
	if err := provider.Delete(context.Background(), "recordings/does-not-exist"); err != nil {
		t.Fatalf("missing delete was not idempotent: %v", err)
	}
	if _, err := provider.Stat(context.Background(), "recordings/does-not-exist"); !errors.Is(err, storageproto.ErrNotFound) {
		t.Fatalf("missing stat returned %v", err)
	}
	if err := provider.Delete(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if err := provider.Delete(context.Background(), key); err != nil {
		t.Fatalf("second delete failed: %v", err)
	}
	if _, _, err := provider.Open(context.Background(), key); !errors.Is(err, storageproto.ErrNotFound) {
		t.Fatalf("deleted object returned %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := provider.Put(canceled, "recordings/canceled", bytes.NewReader([]byte("x")), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled put returned %v", err)
	}
	if _, _, err := provider.Open(canceled, "recordings/canceled"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled open returned %v", err)
	}
	if err := provider.Delete(canceled, key); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled delete returned %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "atomic"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), temporaryPrefix) {
			t.Fatal("temporary object leaked")
		}
	}
}

func TestProbeRejectsConfiguredRootSymlink(t *testing.T) {
	provider := New()
	parent := t.TempDir()
	actual := filepath.Join(parent, "actual")
	if err := os.Mkdir(actual, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(actual, link); err != nil {
		t.Fatal(err)
	}
	if err := provider.Configure(context.Background(), rootConfig(t, link)); err != nil {
		t.Fatal(err)
	}
	if err := provider.Probe(context.Background()); err == nil {
		t.Fatal("configured root symlink was accepted")
	}
}

func configuredProvider(t *testing.T) (*Provider, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "recordings")
	provider := New()
	if err := provider.Configure(context.Background(), rootConfig(t, root)); err != nil {
		t.Fatal(err)
	}
	if err := provider.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	return provider, root
}

func rootConfig(t *testing.T, root string) storageproto.Config {
	t.Helper()
	encoded, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	return storageproto.Config{Values: map[string]json.RawMessage{"root": encoded}}
}

func objectInfo(data []byte) storageproto.ObjectInfo {
	digest := sha256.Sum256(data)
	return storageproto.ObjectInfo{Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
}

func entryKeys(entries []storageproto.ObjectEntry) []string {
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.Key)
	}
	return keys
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

type cancelAfterRead struct {
	cancel context.CancelFunc
	bytes  []byte
	done   bool
}

type cancelAfterErrContext struct {
	context.Context
	cancel   context.CancelFunc
	cancelAt int
	checks   int
}

func (ctx *cancelAfterErrContext) Err() error {
	ctx.checks++
	if ctx.checks == ctx.cancelAt {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func (r *cancelAfterRead) Read(buffer []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	count := copy(buffer, r.bytes)
	r.cancel()
	return count, nil
}

func TestListAndPutRejectInvalidLimitsAndPrivatePrefixExposure(t *testing.T) {
	provider, root := configuredProvider(t)
	payload := []byte("one")
	if _, err := provider.Put(context.Background(), "_integrated-recorder/probe/item", bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{0, storageproto.MaxListLimit + 1} {
		if _, err := provider.List(context.Background(), "", "", limit); err == nil {
			t.Errorf("invalid list limit %d accepted", limit)
		}
	}
	page, err := provider.List(context.Background(), "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range page.Items {
		if strings.Contains(entry.Key, privateDirectory) {
			t.Fatalf("provider-private path leaked into logical list: %q", entry.Key)
		}
	}
	if _, err := os.Stat(filepath.Join(root, privateDirectory, privateObjects)); err != nil {
		t.Fatal(fmt.Errorf("private namespace was not created: %w", err))
	}
}
