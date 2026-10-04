package server

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/jpeg"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/acquire"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/preview"
	"github.com/integrated-recorder/core/internal/storage"
)

func TestPreviewAPIRoutesRequireAuthAndServePrivateJPEG(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("a", 32)
	recording := writeProductRecording(t, store, id, domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := manager.Get(id)
	if err != nil || len(loaded.Tracks["main"].Segments) != 1 {
		t.Fatalf("manager did not reload preview fixture: recording=%+v err=%v", loaded, err)
	}
	jpegPath := filepath.Join(root, "fixture.jpg")
	if err := os.WriteFile(jpegPath, serverValidJPEG(t), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("IR_SERVER_PREVIEW_JPEG", jpegPath)
	ffmpegShim := filepath.Join(root, "ffmpeg-shim")
	if err := os.WriteFile(ffmpegShim, []byte("#!/bin/sh\ncp \"$IR_SERVER_PREVIEW_JPEG\" frame.jpg\n"), 0700); err != nil {
		t.Fatal(err)
	}
	previews, err := preview.OpenWithGet(root, store, manager.List, manager.Get, ffmpegShim)
	if err != nil {
		t.Fatal(err)
	}
	defer previews.Close(context.Background())
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := os.ReadFile(filepath.Join(root, "security", "bootstrap-token"))
	if err != nil {
		t.Fatal(err)
	}
	const password = "correct horse battery staple"
	if err := auth.Bootstrap(strings.TrimSpace(string(bootstrap)), password); err != nil {
		t.Fatal(err)
	}
	api := NewWithOptions(manager, nil, nil, Options{Previews: previews, Auth: auth})
	unauthorized := httptest.NewRecorder()
	api.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/previews", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated preview list status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}
	session, err := auth.Login(password)
	if err != nil {
		t.Fatal(err)
	}
	request := func(method, path, body string, csrf bool) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: session.Token})
		if csrf {
			req.Header.Set("X-CSRF-Token", session.CSRFToken)
		}
		response := httptest.NewRecorder()
		api.ServeHTTP(response, req)
		return response
	}
	withoutCSRF := request(http.MethodPost, "/api/recordings/"+id+"/previews", `{"mode":"segment"}`, false)
	if withoutCSRF.Code != http.StatusForbidden {
		t.Fatalf("preview policy mutation without CSRF status=%d body=%s", withoutCSRF.Code, withoutCSRF.Body.String())
	}
	enabled := request(http.MethodPost, "/api/recordings/"+id+"/previews", `{"mode":"segment"}`, true)
	if enabled.Code != http.StatusAccepted || !strings.Contains(enabled.Body.String(), `"mode":"segment"`) {
		t.Fatalf("preview policy enable status=%d body=%s", enabled.Code, enabled.Body.String())
	}
	deadline := time.Now().Add(5 * time.Second)
	for previews.Summary(recording).FrameCount == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if previews.Summary(recording).FrameCount != 1 {
		t.Fatalf("preview worker did not publish a frame: summary=%+v; files=%v", previews.Summary(recording), directoryEntries(filepath.Join(root, "previews", id)))
	}
	listed := request(http.MethodGet, "/api/recordings/"+id+"/previews?sampling=uniform&limit=48", "", false)
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), `"archive_ordinal":1`) || strings.Contains(listed.Body.String(), "original-media-payload") || strings.Contains(listed.Body.String(), root) {
		t.Fatalf("preview list status=%d body=%s", listed.Code, listed.Body.String())
	}
	imageResponse := request(http.MethodGet, "/api/recordings/"+id+"/previews/1", "", false)
	if imageResponse.Code != http.StatusOK || imageResponse.Header().Get("Content-Type") != "image/jpeg" || imageResponse.Header().Get("X-Content-Type-Options") != "nosniff" || !strings.Contains(imageResponse.Header().Get("Cache-Control"), "private") || imageResponse.Header().Get("ETag") == "" {
		t.Fatalf("preview image headers/status=%d headers=%v", imageResponse.Code, imageResponse.Header())
	}
	if _, format, decodeErr := image.DecodeConfig(bytes.NewReader(imageResponse.Body.Bytes())); decodeErr != nil || format != "jpeg" {
		t.Fatalf("preview response is not a valid JPEG: format=%q err=%v", format, decodeErr)
	}
	missing := request(http.MethodGet, "/api/recordings/"+id+"/previews/2", "", false)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing preview image status=%d body=%s", missing.Code, missing.Body.String())
	}
}

func directoryEntries(path string) []string {
	entries, err := os.ReadDir(path)
	if err != nil {
		return []string{err.Error()}
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func serverValidJPEG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 48, 27))
	for y := 0; y < 27; y++ {
		for x := 0; x < 48; x++ {
			img.Set(x, y, color.RGBA{R: uint8(x * 3), G: uint8(y * 5), B: uint8(x + y), A: 255})
		}
	}
	var encoded bytes.Buffer
	if err := jpeg.Encode(&encoded, img, &jpeg.Options{Quality: 80}); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func TestPreviewCleanupRunsAfterExplicitRecordingDelete(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("b", 32)
	recording := writeProductRecording(t, store, id, domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	previews, err := preview.OpenWithGet(root, store, manager.List, manager.Get, filepath.Join(root, "missing-ffmpeg"))
	if err != nil {
		t.Fatal(err)
	}
	defer previews.Close(context.Background())
	if _, err := previews.SetMode(id, preview.ModeSegment); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "previews", id, "frames"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "previews", id, "frames", "000000000001.jpg"), serverValidJPEG(t), 0600); err != nil {
		t.Fatal(err)
	}
	api := NewWithOptions(manager, nil, nil, Options{Previews: previews})
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/api/recordings/"+id, nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("recording delete status=%d body=%s", response.Code, response.Body.String())
	}
	if _, err := os.Lstat(filepath.Join(root, "previews", id)); !os.IsNotExist(err) {
		t.Fatalf("preview projection remains after recording deletion: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "management", "previews", id+".json")); !os.IsNotExist(err) {
		t.Fatalf("preview policy remains after recording deletion: %v", err)
	}
	if _, err := manager.Get(recording.ID); err == nil {
		t.Fatal("canonical recording unexpectedly remains after delete")
	}
	frameResponse := httptest.NewRecorder()
	api.ServeHTTP(frameResponse, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id+"/previews/1", nil))
	if frameResponse.Code != http.StatusNotFound {
		t.Fatalf("deleted recording preview endpoint status=%d body=%s", frameResponse.Code, frameResponse.Body.String())
	}
}

func TestPreviewPolicySummaryIsSmallAndDefaultDisabled(t *testing.T) {
	root := t.TempDir()
	store, err := storage.New(root)
	if err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("c", 32)
	recording := writeProductRecording(t, store, id, domain.StateCompleted)
	manager, err := acquire.NewManager(store, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	previews, err := preview.OpenWithGet(root, store, manager.List, manager.Get, filepath.Join(root, "missing-ffmpeg"))
	if err != nil {
		t.Fatal(err)
	}
	defer previews.Close(context.Background())
	api := NewWithOptions(manager, nil, nil, Options{Previews: previews})
	response := httptest.NewRecorder()
	api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/recordings/"+id, nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"preview":{"mode":"disabled","state":"disabled","available":false,"frame_count":0}`) {
		t.Fatalf("default preview summary status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "items") || strings.Contains(response.Body.String(), "image_sha256") {
		t.Fatalf("recording detail included full preview index: %s", response.Body.String())
	}
	_ = recording
}
