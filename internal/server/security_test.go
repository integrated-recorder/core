package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestStrictJSONMutationEndpointsRejectInvalidDocuments(t *testing.T) {
	handler := New(nil, nil, nil)
	paths := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/api/recordings"},
		{http.MethodPut, "/api/adapters/example/config"},
		{http.MethodPost, "/api/resolve-workflows/example/continue"},
	}
	invalid := map[string]string{
		"empty":              "",
		"unknown field":      `{"unexpected":true}`,
		"second JSON object": `{} {}`,
		"trailing garbage":   `{} trailing`,
		"oversized":          `{"padding":"` + strings.Repeat("x", 70<<10) + `"}`,
	}
	for _, endpoint := range paths {
		for name, body := range invalid {
			t.Run(endpoint.method+" "+endpoint.path+"/"+name, func(t *testing.T) {
				request := httptest.NewRequest(endpoint.method, endpoint.path, strings.NewReader(body))
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != http.StatusBadRequest {
					t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
				}
			})
		}
	}
}

func TestManagementAssetsAreLocalAndSecurityHeadersArePresent(t *testing.T) {
	handler := New(nil, nil, nil)
	page := httptest.NewRecorder()
	handler.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/", nil))
	if page.Code != http.StatusOK {
		t.Fatalf("page status=%d", page.Code)
	}
	for key, want := range map[string]string{
		"Content-Security-Policy": "script-src 'self'",
		"X-Content-Type-Options":  "nosniff",
		"Referrer-Policy":         "no-referrer",
		"X-Frame-Options":         "DENY",
	} {
		if got := page.Header().Get(key); !strings.Contains(got, want) {
			t.Errorf("%s=%q, want it to contain %q", key, got, want)
		}
	}
	csp := page.Header().Get("Content-Security-Policy")
	if !strings.Contains(csp, "script-src 'self'; style-src 'self'; style-src-attr 'unsafe-inline'") {
		t.Fatalf("CSP must allow component positioning styles without allowing inline scripts: %q", csp)
	}
	if strings.Contains(page.Body.String(), "cdn.jsdelivr.net") || strings.Contains(page.Body.String(), "<script>") || !strings.Contains(page.Body.String(), "/static/ui/assets/") {
		t.Fatal("management page loads unpinned external or inline script")
	}
	deepRoute := httptest.NewRecorder()
	deepRequest := httptest.NewRequest(http.MethodGet, "/recordings/recording-id", nil)
	deepRequest.Header.Set("Accept", "text/html")
	handler.ServeHTTP(deepRoute, deepRequest)
	if deepRoute.Code != http.StatusOK || !strings.Contains(deepRoute.Body.String(), "/static/ui/assets/") {
		t.Fatalf("direct SPA route did not serve application shell: %d", deepRoute.Code)
	}
}

func TestSegmentResponsesArePrivateAndHaveCompleteContentTypes(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	const id = "0123456789abcdef0123456789abcdef"
	now := time.Now().UTC()
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateStopped, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	for index, ext := range []string{".mp3", ".vtt"} {
		segmentID := fmt.Sprintf("segment-%d", index)
		path := filepath.ToSlash(filepath.Join("tracks", "main", segmentID+ext))
		payload := []byte("payload-" + ext)
		result, saveErr := store.SavePayload(id, path, bytes.NewReader(payload), 1024)
		if saveErr != nil {
			t.Fatal(saveErr)
		}
		recording.Tracks["main"].Segments = append(recording.Tracks["main"].Segments, domain.Segment{ID: segmentID, TrackID: "main", Sequence: uint64(index), Duration: 1, StoragePath: path, PayloadSize: result.Size, SHA256: result.SHA256})
	}
	if err = store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	handler := New(manager, nil, nil)
	for index, want := range []string{"audio/mpeg", "text/vtt; charset=utf-8"} {
		request := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/recordings/%s/play/segments/segment-%d", id, index), nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("segment %d status=%d body=%s", index, response.Code, response.Body.String())
		}
		if got := response.Header().Get("Cache-Control"); !strings.HasPrefix(got, "private,") || strings.Contains(got, "public") {
			t.Errorf("segment cache policy=%q", got)
		}
		if !strings.Contains(response.Header().Get("Cache-Control"), "no-store") {
			t.Errorf("segment response is cacheable: %q", response.Header().Get("Cache-Control"))
		}
		if got := response.Header().Get("Content-Type"); got != want {
			t.Errorf("segment content type=%q, want %q", got, want)
		}
	}
	api := httptest.NewRecorder()
	handler.ServeHTTP(api, httptest.NewRequest(http.MethodGet, "/api/recordings", nil))
	if api.Code != http.StatusOK || api.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("recording API cache policy: status=%d cache-control=%q", api.Code, api.Header().Get("Cache-Control"))
	}
}

type writeDeadlineResponseWriter struct {
	header    http.Header
	body      bytes.Buffer
	status    int
	deadlines []time.Time
}

func (w *writeDeadlineResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *writeDeadlineResponseWriter) WriteHeader(status int) { w.status = status }

func (w *writeDeadlineResponseWriter) Write(data []byte) (int, error) { return w.body.Write(data) }

func (w *writeDeadlineResponseWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return nil
}

func TestSegmentHandlerClearsServerWriteDeadline(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "0123456789abcdef0123456789abcdef"
	now := time.Now().UTC()
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateStopped, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	payload := []byte("original segment bytes")
	path := "tracks/main/segment.ts"
	result, err := store.SavePayload(id, path, bytes.NewReader(payload), 1024)
	if err != nil {
		t.Fatal(err)
	}
	recording.Tracks["main"].Segments = append(recording.Tracks["main"].Segments, domain.Segment{ID: "segment", TrackID: "main", StoragePath: path, PayloadSize: result.Size, SHA256: result.SHA256})
	if err = store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	response := &writeDeadlineResponseWriter{}
	handler := New(manager, nil, nil)
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/play/segments/segment", nil))
	if (response.status != 0 && response.status != http.StatusOK) || !bytes.Equal(response.body.Bytes(), payload) {
		t.Fatalf("segment response status=%d body=%q", response.status, response.body.Bytes())
	}
	if len(response.deadlines) != 1 || !response.deadlines[0].IsZero() {
		t.Fatalf("segment write deadlines=%v, want one cleared deadline", response.deadlines)
	}
}

func TestCanonicalPayloadRecoveryFailsClosedForVOD(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "fedcba0987654321fedcba0987654321"
	now := time.Now().UTC()
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateStopped, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: []domain.Segment{{ID: "missing", TrackID: "main", Sequence: 1, ArchiveOrdinal: 1, Duration: 10, StoragePath: "tracks/main/missing.ts", PayloadSize: 5, SHA256: strings.Repeat("a", 64)}}}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	if !store.HasCanonicalPayloadIssue(id) {
		t.Fatal("missing canonical payload was not indexed during reload")
	}
	handler := New(manager, nil, nil)
	for _, path := range []string{
		"/api/recordings/" + id + "/play/master.m3u8",
		"/api/recordings/" + id + "/play/tracks/main/playlist.m3u8",
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusServiceUnavailable || response.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("%s status=%d cache=%q body=%s", path, response.Code, response.Header().Get("Cache-Control"), response.Body.String())
		}
		if strings.Contains(response.Body.String(), "missing.ts") || strings.Contains(response.Body.String(), "aaaaaaaa") {
			t.Errorf("%s leaked unavailable source metadata: %s", path, response.Body.String())
		}
	}
	segment := httptest.NewRecorder()
	handler.ServeHTTP(segment, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/play/segments/missing", nil))
	if segment.Code != http.StatusNotFound {
		t.Fatalf("missing payload segment status=%d, want 404", segment.Code)
	}
}

func TestVODUsesArchiveOrdinalAcrossSourceSequenceReset(t *testing.T) {
	store, err := storage.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const id = "fedcba0987654321fedcba0987654321"
	now := time.Now().UTC()
	segments := []domain.Segment{
		{ID: "a", TrackID: "main", SourceEpoch: 0, ArchiveOrdinal: 1, Sequence: 1000, Duration: 1, StoragePath: "tracks/main/a.ts", PayloadSize: 1},
		{ID: "b", TrackID: "main", SourceEpoch: 0, ArchiveOrdinal: 2, Sequence: 1001, Duration: 1, StoragePath: "tracks/main/b.ts", PayloadSize: 1},
		{ID: "c", TrackID: "main", SourceEpoch: 0, DiscontinuitySequence: 1, ArchiveOrdinal: 3, Sequence: 1002, Duration: 1, StoragePath: "tracks/main/c.ts", PayloadSize: 1},
	}
	recording := &domain.Recording{FormatVersion: 1, ID: id, State: domain.StateStopped, CreatedAt: now, StartedAt: now, Tracks: map[string]*domain.Track{"main": {ID: "main", Segments: segments}}}
	if err = store.CreateRecording(recording); err != nil {
		t.Fatal(err)
	}
	for index := range segments {
		result, saveErr := store.SavePayload(id, segments[index].StoragePath, strings.NewReader("x"), 1024)
		if saveErr != nil {
			err = saveErr
			t.Fatal(err)
		}
		segments[index].PayloadSize = result.Size
		segments[index].SHA256 = result.SHA256
	}
	recording.Tracks["main"].Segments = segments
	if err = store.SaveRecording(recording); err != nil {
		t.Fatal(err)
	}
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(context.Background())
	handler := New(manager, nil, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/play/tracks/main/playlist.m3u8", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("playlist status=%d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	if strings.Index(body, "/segments/a\n") > strings.Index(body, "/segments/b\n") || strings.Index(body, "/segments/b\n") > strings.Index(body, "/segments/c\n") {
		t.Fatalf("VOD order does not follow archive order:\n%s", body)
	}
	if !strings.Contains(body, "#EXT-X-DISCONTINUITY") || !strings.Contains(body, "#EXT-X-MEDIA-SEQUENCE:0") {
		t.Fatalf("archive order/discontinuity sequence projection is incomplete:\n%s", body)
	}
}

func TestStrictJSONDecoderRequiresExactlyOneValue(t *testing.T) {
	for _, body := range []string{`{}`, "{} \n", `{} {}`, `{}garbage`, ``} {
		t.Run(fmt.Sprintf("%q", body), func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			response := httptest.NewRecorder()
			var value map[string]json.RawMessage
			err := decodeJSONBody(response, request, 1024, &value)
			valid := body == `{}` || body == "{} \n"
			if (err == nil) != valid {
				t.Fatalf("decode error=%v", err)
			}
		})
	}
}

func TestSegmentMetadataDoesNotExposeSourceURI(t *testing.T) {
	var detailView recordingDetail
	secretURL := "https://media.example/seg.ts?token=must-not-appear"
	detailView.Recording = &domain.Recording{SourceURIClassification: "sensitive", Tracks: map[string]*domain.Track{"main": {ID: "main", SourcePlaylistURL: secretURL, Segments: []domain.Segment{{ID: "seg", SourceURI: secretURL}}}}}
	data, err := json.Marshal(detail(detailView.Recording))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "must-not-appear") || strings.Contains(string(data), "media.example") || strings.Contains(string(data), "acquisition_context") {
		t.Fatalf("detail leaked a sensitive source URI: %s", data)
	}
}
