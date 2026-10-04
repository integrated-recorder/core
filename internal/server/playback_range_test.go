package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

const playbackRangeRecordingID = "0123456789abcdef0123456789abcdef"

var playbackRangePayload = []byte("0123456789")

func newPlaybackRangeHandler(t *testing.T, objects storage.PhysicalObjectStore) (http.Handler, *rangeTrackingObjects) {
	t.Helper()
	var tracked *rangeTrackingObjects
	var store *storage.Store
	var err error
	if objects == nil {
		store, err = storage.New(t.TempDir())
	} else {
		tracked = objects.(*rangeTrackingObjects)
		store, err = storage.NewWithObjectStore(t.TempDir(), tracked)
	}
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	recording := &domain.Recording{
		FormatVersion: 1,
		ID:            playbackRangeRecordingID,
		State:         domain.StateStopped,
		CreatedAt:     now,
		StartedAt:     now,
		Tracks:        map[string]*domain.Track{"main": {ID: "main"}},
	}
	if err := store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	const path = "tracks/main/segment.m4s"
	result, err := store.SavePayload(recording.ID, path, bytes.NewReader(playbackRangePayload), int64(len(playbackRangePayload)))
	if err != nil {
		t.Fatal(err)
	}
	segment := domain.Segment{
		ID:          "segment-range",
		TrackID:     "main",
		Sequence:    1,
		StoragePath: path,
		PayloadSize: result.Size,
		SHA256:      result.SHA256,
	}
	if err := store.SaveSidecar(recording.ID, path, segment); err != nil {
		t.Fatal(err)
	}
	recording.Tracks["main"].Segments = []domain.Segment{segment}
	if err := store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close(context.Background()) })
	return New(manager, nil, nil), tracked
}

func TestSegmentRangeRequestsReturnOnlyRequestedBytes(t *testing.T) {
	handler, _ := newPlaybackRangeHandler(t, nil)
	for _, test := range []struct {
		name       string
		header     string
		start, end int
	}{
		{name: "closed", header: "bytes=2-5", start: 2, end: 6},
		{name: "open ended", header: "bytes=6-", start: 6, end: 10},
		{name: "suffix", header: "bytes=-3", start: 7, end: 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/api/recordings/"+playbackRangeRecordingID+"/play/segments/segment-range", nil)
			request.Header.Set("Range", test.header)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			want := playbackRangePayload[test.start:test.end]
			if response.Code != http.StatusPartialContent || !bytes.Equal(response.Body.Bytes(), want) {
				t.Fatalf("status=%d body=%q, want 206 %q", response.Code, response.Body.Bytes(), want)
			}
			if got := response.Header().Get("Accept-Ranges"); got != "bytes" {
				t.Errorf("Accept-Ranges=%q", got)
			}
			if got, want := response.Header().Get("Content-Range"), fmt.Sprintf("bytes %d-%d/%d", test.start, test.end-1, len(playbackRangePayload)); got != want {
				t.Errorf("Content-Range=%q, want %q", got, want)
			}
			if got, want := response.Header().Get("Content-Length"), fmt.Sprint(len(want)); got != want {
				t.Errorf("Content-Length=%q, want %q", got, want)
			}
			if !strings.Contains(response.Header().Get("Cache-Control"), "no-store") {
				t.Errorf("Cache-Control=%q", response.Header().Get("Cache-Control"))
			}
		})
	}
}

func TestSegmentRangeInvalidRequestsReturn416WithoutPayload(t *testing.T) {
	handler, _ := newPlaybackRangeHandler(t, nil)
	for _, header := range []string{
		"bytes=99-100",
		"bytes=5-2",
		"bytes=-0",
		"bytes=0-1,4-5",
		"items=0-1",
		"bytes=999999999999999999999-",
		"bytes=0- 1",
	} {
		t.Run(header, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/api/recordings/"+playbackRangeRecordingID+"/play/segments/segment-range", nil)
			request.Header.Set("Range", header)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusRequestedRangeNotSatisfiable {
				t.Fatalf("status=%d body=%q, want 416", response.Code, response.Body.Bytes())
			}
			if got, want := response.Header().Get("Content-Range"), "bytes */10"; got != want {
				t.Errorf("Content-Range=%q, want %q", got, want)
			}
			if response.Body.Len() != 0 {
				t.Errorf("invalid range returned payload %q", response.Body.Bytes())
			}
		})
	}
}

func TestSegmentFullGETRemains200AndRemoteRangeUsesOpenRange(t *testing.T) {
	objects := newRangeTrackingObjects()
	handler, tracked := newPlaybackRangeHandler(t, objects)
	full := httptest.NewRecorder()
	handler.ServeHTTP(full, httptest.NewRequest(http.MethodGet, "/api/recordings/"+playbackRangeRecordingID+"/play/segments/segment-range", nil))
	if full.Code != http.StatusOK || !bytes.Equal(full.Body.Bytes(), playbackRangePayload) {
		t.Fatalf("full GET status=%d body=%q", full.Code, full.Body.Bytes())
	}
	if got := full.Header().Get("Content-Length"); got != fmt.Sprint(len(playbackRangePayload)) {
		t.Fatalf("full GET Content-Length=%q", got)
	}
	if got := full.Header().Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("full GET Accept-Ranges=%q", got)
	}

	tracked.mu.Lock()
	tracked.openCalls = 0
	tracked.rangeCalls = nil
	tracked.mu.Unlock()
	partial := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/recordings/"+playbackRangeRecordingID+"/play/segments/segment-range", nil)
	request.Header.Set("Range", "bytes=3-5")
	handler.ServeHTTP(partial, request)
	if partial.Code != http.StatusPartialContent || partial.Body.String() != "345" {
		t.Fatalf("remote range status=%d body=%q", partial.Code, partial.Body.String())
	}
	tracked.mu.Lock()
	defer tracked.mu.Unlock()
	if tracked.openCalls != 0 {
		t.Fatalf("remote range fell back to full Open %d times", tracked.openCalls)
	}
	if len(tracked.rangeCalls) != 1 || tracked.rangeCalls[0].Offset != 3 || tracked.rangeCalls[0].Length != 3 {
		t.Fatalf("remote OpenRange calls=%#v", tracked.rangeCalls)
	}
}

type rangeTrackingObjects struct {
	mu         sync.Mutex
	objects    map[string][]byte
	openCalls  int
	rangeCalls []playbackPhysicalRangeCall
}

type playbackPhysicalRangeCall struct {
	Offset int64
	Length int64
}

func newRangeTrackingObjects() *rangeTrackingObjects {
	return &rangeTrackingObjects{objects: make(map[string][]byte)}
}

func (s *rangeTrackingObjects) Put(ctx context.Context, key string, body io.Reader, size int64) (storage.PhysicalObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return storage.PhysicalObjectInfo{}, err
	}
	data, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil || int64(len(data)) != size {
		return storage.PhysicalObjectInfo{}, errors.New("invalid object body")
	}
	s.mu.Lock()
	s.objects[key] = append([]byte(nil), data...)
	info := s.infoLocked(key)
	s.mu.Unlock()
	return info, nil
}

func (s *rangeTrackingObjects) Open(ctx context.Context, key string) (io.ReadCloser, storage.PhysicalObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, storage.PhysicalObjectInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		return nil, storage.PhysicalObjectInfo{}, storage.ErrObjectNotFound
	}
	s.openCalls++
	return io.NopCloser(bytes.NewReader(append([]byte(nil), data...))), s.infoLocked(key), nil
}

func (s *rangeTrackingObjects) OpenRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storage.PhysicalObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, storage.PhysicalObjectInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		return nil, storage.PhysicalObjectInfo{}, storage.ErrObjectNotFound
	}
	if offset < 0 || length <= 0 || offset > int64(len(data))-length {
		return nil, storage.PhysicalObjectInfo{}, errors.New("invalid object range")
	}
	s.rangeCalls = append(s.rangeCalls, playbackPhysicalRangeCall{Offset: offset, Length: length})
	return io.NopCloser(bytes.NewReader(append([]byte(nil), data[offset:offset+length]...))), s.infoLocked(key), nil
}

func (s *rangeTrackingObjects) Stat(ctx context.Context, key string) (storage.PhysicalObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return storage.PhysicalObjectInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[key]; !ok {
		return storage.PhysicalObjectInfo{}, storage.ErrObjectNotFound
	}
	return s.infoLocked(key), nil
}

func (s *rangeTrackingObjects) List(ctx context.Context, prefix, cursor string, limit int) (storage.PhysicalObjectPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.PhysicalObjectPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) && key > cursor {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if limit <= 0 || limit > len(keys) {
		limit = len(keys)
	}
	page := storage.PhysicalObjectPage{Items: make([]storage.PhysicalObjectInfo, 0, limit)}
	for _, key := range keys[:limit] {
		page.Items = append(page.Items, s.infoLocked(key))
	}
	if limit < len(keys) && limit > 0 {
		page.NextCursor = keys[limit-1]
	}
	return page, nil
}

func (s *rangeTrackingObjects) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[key]; !ok {
		return storage.ErrObjectNotFound
	}
	delete(s.objects, key)
	return nil
}

func (s *rangeTrackingObjects) infoLocked(key string) storage.PhysicalObjectInfo {
	data := s.objects[key]
	digest := sha256.Sum256(data)
	return storage.PhysicalObjectInfo{Key: key, Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:]), ModifiedAt: time.Now().UTC()}
}
