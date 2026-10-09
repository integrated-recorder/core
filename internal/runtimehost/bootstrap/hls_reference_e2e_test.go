package bootstrap

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
)

// TestProductionBundledHLSReferenceRecordingE2E drives the real bundled HLS
// adapter through Runtime Host, Control, and Recorder Engine processes. It
// verifies that direct manifest input reaches the canonical archive and the
// normal VOD segment routes without changing media bytes.
func TestProductionBundledHLSReferenceRecordingE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("requires building and running production Runtime Host, Control Plane, and Recorder Engine")
	}
	if runtime.GOOS == "windows" {
		t.Skip("process-tree assertions currently use the Unix ps interface")
	}

	fixture := newRuntimeUpdateFixture(t)
	artifacts := buildRuntimeAdapterLifecycleArtifacts(t, fixture.server.URL)
	const stream = "bundled-hls-reference"
	fixture.reset(stream, "Direct HLS fixture", "Bundled HLS reference recording", "hls-session")
	fixture.advance(stream, 5)

	dataRoot := newRuntimeE2ETempDir(t)
	dataDir := filepath.Join(dataRoot, "data")
	operatorAdapterDir := filepath.Join(dataRoot, "operator-adapters")
	for _, directory := range []string{dataDir, operatorAdapterDir} {
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatalf("create empty Runtime Host directory: %v", err)
		}
	}
	t.Cleanup(func() { makeRuntimeE2ETreeWritable(dataDir) })

	listenAddr := reserveRuntimeAddress(t)
	command := exec.Command(artifacts.hostA)
	command.Env = minimalRuntimeE2EEnv([]string{
		"DATA_DIR=" + dataDir,
		"ADDR=" + listenAddr,
		"ADAPTER_DIR=" + operatorAdapterDir,
		"IR_STORAGE_LOCAL_PLUGIN=" + artifacts.storageLocalBinary,
		"AUTH_DISABLED=1",
	})
	process := &runtimeHostProcess{command: command, done: make(chan struct{})}
	command.Stdout, command.Stderr = &process.output, &process.output
	if err := command.Start(); err != nil {
		t.Fatalf("start production Runtime Host: %v", err)
	}
	go func() {
		process.err = command.Wait()
		close(process.done)
	}()

	controlBinary := filepath.Join(artifacts.bundleA, "control-plane")
	engineBinary := filepath.Join(artifacts.bundleA, "recorder-engine")
	var immutableHLSBinary string
	t.Cleanup(func() {
		stopRuntimeHostProcess(process)
		if t.Failed() {
			t.Logf("production Runtime Host output: %s", process.output.String())
		}
		for _, executable := range []string{artifacts.hostA, controlBinary, engineBinary, immutableHLSBinary} {
			if executable == "" {
				continue
			}
			if err := waitProcessAbsent(t, executable, 15*time.Second); err != nil {
				t.Errorf("acceptance cleanup left product process running for %s: %v; host output: %s", filepath.Base(executable), err, process.output.String())
			}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	baseURL := "http://" + listenAddr
	client := &http.Client{Timeout: 30 * time.Second}
	status := waitRuntimeHostStatus(t, ctx, client, baseURL, process)
	if status.ActiveControl == nil || status.DefaultEngine == nil {
		t.Fatalf("production Runtime Host did not activate Control and Engine: status=%+v; host output: %s", status, process.output.String())
	}
	hostPID := command.Process.Pid
	controlPID := waitDirectChildForBinary(t, hostPID, controlBinary, 20*time.Second)
	enginePID := waitDirectChildForBinary(t, hostPID, engineBinary, 20*time.Second)
	if controlPID == enginePID || controlPID == hostPID || enginePID == hostPID {
		t.Fatalf("Host/Control/Engine are not separate production processes: host=%d control=%d engine=%d", hostPID, controlPID, enginePID)
	}

	var adapters []struct {
		Descriptor *struct {
			ID      string `json:"id"`
			Version string `json:"version"`
		} `json:"descriptor"`
		Trust *struct {
			Provenance string `json:"provenance"`
			Authority  string `json:"authority"`
			Publisher  string `json:"publisher"`
			Reviewed   bool   `json:"reviewed"`
		} `json:"trust"`
	}
	code, err := requestRuntimeJSON(client, baseURL, http.MethodGet, "/api/adapters", nil, &adapters)
	if err != nil || code != http.StatusOK {
		t.Fatalf("GET /api/adapters returned code=%d err=%v; host output: %s", code, err, process.output.String())
	}
	foundHLS, foundOwncast := false, false
	for _, adapter := range adapters {
		if adapter.Descriptor == nil {
			continue
		}
		switch adapter.Descriptor.ID {
		case "hls":
			foundHLS = true
			if adapter.Trust == nil || adapter.Trust.Provenance != "bundled" || adapter.Trust.Authority != "core_release" || adapter.Trust.Publisher != "first_party" || !adapter.Trust.Reviewed {
				t.Fatalf("HLS adapter did not project Host-assigned bundled trust: trust=%+v", adapter.Trust)
			}
		case "owncast":
			foundOwncast = true
		}
	}
	if !foundHLS || foundOwncast {
		t.Fatalf("adapter projection must include bundled HLS and omit Owncast: hls=%t owncast=%t adapters=%+v", foundHLS, foundOwncast, adapters)
	}

	snapshot := readRuntimeGenerationSnapshot(t, dataDir)
	generation, exists := snapshot.Generations[status.DefaultEngine.ID]
	if !exists || generation.AdapterSetID == "" {
		t.Fatalf("active generation does not pin an immutable adapter set: generation=%+v snapshot=%+v", generation, snapshot)
	}
	immutableHLSBinary = filepath.Join(dataDir, "runtime", "adapters", "sets", generation.AdapterSetID, "bin", "integrated-recorder-adapter-hls")
	if info, statErr := os.Lstat(immutableHLSBinary); statErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0222 != 0 {
		t.Fatalf("active immutable adapter set does not contain a non-writable HLS executable: info=%v err=%v", info, statErr)
	}

	manifestURL := fixture.server.URL + "/hls/stream.m3u8?stream=" + urlQueryEscape(stream) + "&token=token-0"
	recording := createRuntimeRecordingWithAdapter(t, client, baseURL, "hls", "Bundled HLS direct manifest", map[string]string{
		"manifest_url": manifestURL,
	})
	if recording.ID == "" {
		t.Fatal("Control API returned an empty recording ID")
	}
	waitRecordingSequenceCount(t, client, baseURL, recording.ID, 5, 30*time.Second)
	stopRecording(t, client, baseURL, recording.ID)
	waitRuntimeRecordingState(t, client, baseURL, recording.ID, domain.StateStopped, 20*time.Second)
	final := getRecording(t, dataDir, client, baseURL, recording.ID)
	if final.ID != recording.ID || final.State != domain.StateStopped {
		t.Fatalf("stop changed Recording identity or did not finalize it: created=%s final=%+v", recording.ID, final)
	}
	track := final.Tracks["main"]
	if track == nil || len(track.Segments) < 5 {
		t.Fatalf("direct HLS recording has fewer than five canonical segments: track=%+v", track)
	}
	sequences := recordingSequences(final)
	if len(final.Gaps) != 0 || !equalSequenceRange(sequences, 1, uint64(len(track.Segments))) {
		t.Fatalf("direct HLS source sequences are not a gap-free 1..N range: sequences=%v gaps=%+v", sequences, final.Gaps)
	}
	verifyRuntimeRecordingSegments(t, dataDir, final, stream, 1, uint64(len(track.Segments)))

	masterPath := "/api/recordings/" + recording.ID + "/play/master.m3u8"
	master, code := getRuntimeBody(t, client, baseURL, masterPath)
	if code != http.StatusOK || !strings.Contains(string(master), "/api/recordings/"+recording.ID+"/play/tracks/main/playlist.m3u8") {
		t.Fatalf("VOD master playlist did not expose the canonical main track: status=%d body=%q", code, master)
	}
	trackPath := "/api/recordings/" + recording.ID + "/play/tracks/main/playlist.m3u8"
	playlist, code := getRuntimeBody(t, client, baseURL, trackPath)
	if code != http.StatusOK {
		t.Fatalf("VOD main-track playlist returned %d: %q", code, playlist)
	}
	sequenceByID := make(map[string]uint64, len(track.Segments))
	for _, segment := range track.Segments {
		sequenceByID[segment.ID] = segment.Sequence
	}
	var segmentRoutes []string
	for _, line := range strings.Split(string(playlist), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "/api/recordings/"+recording.ID+"/play/segments/") {
			segmentRoutes = append(segmentRoutes, line)
		}
	}
	if len(segmentRoutes) != len(track.Segments) {
		t.Fatalf("VOD playlist segment route count=%d, canonical segment count=%d; playlist=%q", len(segmentRoutes), len(track.Segments), playlist)
	}
	indices := []int{0, len(segmentRoutes) / 2, len(segmentRoutes) - 1}
	for _, index := range indices {
		route := segmentRoutes[index]
		segmentID := strings.TrimPrefix(route, "/api/recordings/"+recording.ID+"/play/segments/")
		sequence, exists := sequenceByID[segmentID]
		if !exists {
			t.Fatalf("VOD route does not refer to a canonical segment: %q", route)
		}
		payload, statusCode := getRuntimeBody(t, client, baseURL, route)
		want := runtimeSegmentPayload(stream, sequence)
		if statusCode != http.StatusOK || string(payload) != want {
			t.Fatalf("VOD segment read at index %d (sequence %d) differs from original source bytes: status=%d got=%q want=%q", index, sequence, statusCode, payload, want)
		}
	}
}

func getRuntimeBody(t *testing.T, client *http.Client, baseURL, path string) ([]byte, int) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, baseURL+path, nil)
	if err != nil {
		t.Fatalf("build GET %s request: %v", path, err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		t.Fatalf("read GET %s response: %v", path, err)
	}
	return body, response.StatusCode
}
