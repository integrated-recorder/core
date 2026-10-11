package bootstrap

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/archiveindex"
	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/runtimehost/installation"
	"github.com/integrated-recorder/core/internal/storage"
)

// TestProductionHistoricalRepairAcceptanceE2E drives automatic declared-history
// repair through production Runtime Host, Control, Engine, and storage-provider
// processes. The source is deterministic and loopback-only under runtime_e2e;
// production SSRF and private-network policy are unchanged.
func TestProductionHistoricalRepairAcceptanceE2E(t *testing.T) {
	if os.Getenv("IR_RUN_PRODUCTION_HISTORICAL_E2E") != "1" {
		t.Skip("set IR_RUN_PRODUCTION_HISTORICAL_E2E=1 to run the production historical-repair acceptance")
	}
	if runtime.GOOS == "windows" {
		t.Skip("process assertions currently use Unix process controls")
	}
	fixture := newRuntimeUpdateFixture(t)
	artifacts := buildRuntimeUpdateArtifacts(t, fixture.server.URL)
	runProductionHistoricalRepairScenario(t, artifacts, fixture)
}

func runProductionHistoricalRepairScenario(t *testing.T, artifacts runtimeUpdateArtifacts, fixture *runtimeUpdateFixture) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	dataRoot := newRuntimeE2ETempDir(t)
	dataDir := filepath.Join(dataRoot, "data")
	diagnosticDir := filepath.Join(dataRoot, "child-diagnostics")
	if err := os.Mkdir(dataDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(diagnosticDir, 0700); err != nil {
		t.Fatal(err)
	}
	stream := "stage6-history"
	fixture.reset(stream, "Historical repair fixture", "Deterministic history", "session-stage6-history")
	fixture.setLiveFirstSequence(stream, 1001)
	fixture.setHistoricalManifest(stream, historicalRepairManifest(fixture.server.URL, stream, 129, false))
	firstStarted, releaseFirst := fixture.blockHistoricalManifest(stream, 1)
	var releaseSecond func()
	t.Cleanup(func() {
		releaseFirst()
		if releaseSecond != nil {
			releaseSecond()
		}
	})

	controlBinary := filepath.Join(artifacts.bundleA, "control-plane")
	engineBinary := filepath.Join(artifacts.bundleA, "recorder-engine")
	fixtureAdapterBinary := filepath.Join(artifacts.adapterDir, "integrated-recorder-adapter-runtime-update-fixture")
	listenAddr := reserveRuntimeAddress(t)
	command := exec.Command(artifacts.hostA)
	command.Env = minimalRuntimeE2EEnv([]string{
		"DATA_DIR=" + dataDir,
		"ADDR=" + listenAddr,
		"AUTH_DISABLED=1",
		"ADAPTER_DIR=" + artifacts.adapterDir,
		"IR_ALLOW_OPERATOR_PLUGINS=1",
		"IR_STORAGE_LOCAL_PLUGIN=" + artifacts.storageLocalBinary,
		"IR_RELEASE_BUNDLE_DIR=" + artifacts.packageB,
		"IR_RELEASE_TRUSTED_KEYS_JSON=" + artifacts.publicKeys,
		"IR_RUNTIME_E2E_MARKER_DIR=" + diagnosticDir,
	})
	process := &runtimeHostProcess{command: command, done: make(chan struct{}), diagnosticDir: diagnosticDir}
	command.Stdout, command.Stderr = &process.output, &process.output
	if err := command.Start(); err != nil {
		t.Fatalf("start production Runtime Host: %v", err)
	}
	go func() {
		process.err = command.Wait()
		close(process.done)
	}()
	var localStorageArtifact string
	t.Cleanup(func() {
		stopRuntimeHostProcess(process)
		executables := []string{artifacts.hostA, controlBinary, engineBinary, fixtureAdapterBinary}
		if localStorageArtifact != "" {
			executables = append(executables, localStorageArtifact)
		}
		for _, executable := range executables {
			if err := waitProcessAbsent(t, executable, 15*time.Second); err != nil {
				t.Errorf("historical acceptance cleanup left product process running for %s: %v", filepath.Base(executable), err)
			}
		}
	})
	baseURL := "http://" + listenAddr
	client := &http.Client{Timeout: 3 * time.Minute}
	status := waitRuntimeHostStatus(t, ctx, client, baseURL, process)
	if install := installation.ReadOnly(dataDir); install.State != installation.StateReady || install.InstallationID == "" {
		t.Fatalf("AUTH_DISABLED bootstrap did not durably initialize installation: %+v", install)
	}
	if status.DefaultEngine == nil || status.DefaultEngine.ID == "" {
		t.Fatalf("production Engine is not active: %+v", status)
	}
	localStorageArtifact = pinnedLocalStorageArtifact(t, dataDir, status.DefaultEngine.ID)
	recording := createRuntimeRecording(t, client, baseURL, "Stage 6 historical repair", map[string]string{
		"source_url": artifacts.fixtureURL + "/source/" + stream + "?historical=1",
	})
	recordingID := recording.ID

	select {
	case <-firstStarted:
	case <-time.After(40 * time.Second):
		t.Fatalf("automatic historical recovery did not request the declared test manifest: %s", fixture.describe(stream))
	}
	// Keep the historical manifest blocked while two new live segments arrive.
	// This proves the production Engine continues live acquisition at priority.
	fixture.setOnline(stream, true)
	if got := fixture.advance(stream, 2); got != 1002 {
		t.Fatalf("fixture live high-water=%d, want 1002", got)
	}
	waitRecordingSequenceCount(t, client, baseURL, recordingID, 2, 60*time.Second)
	if err := waitShardedHeaderMediaCount(t, dataDir, recordingID, 2, 60*time.Second); err != nil {
		t.Fatalf("live media did not commit while historical manifest was blocked: %v", err)
	}
	preRepairPlaylist := getHistoricalLivePlaylist(t, client, baseURL, recordingID)
	if !strings.Contains(preRepairPlaylist, "#EXT-X-MEDIA-SEQUENCE:") {
		t.Fatalf("initial active live playlist has no MEDIA-SEQUENCE: %q", preRepairPlaylist)
	}
	if len(livePlaylistURIs(preRepairPlaylist)) != 2 {
		t.Fatalf("initial active live playlist does not contain exactly two media URIs: %s", preRepairPlaylist)
	}
	preRepairHeader := loadHistoricalHeader(t, dataDir, recordingID)

	// Request 1 already captured the original all-gap manifest. Change the
	// fixture now so request 2 sees sequence 2 present and sequence 129 newly
	// advertised as a gap.
	fixture.setHistoricalManifest(stream, historicalRepairManifest(fixture.server.URL, stream, 129, true))
	secondStarted, release := fixture.blockHistoricalManifest(stream, 2)
	releaseSecond = release
	releaseFirst()
	select {
	case <-secondStarted:
	case <-time.After(150 * time.Second):
		snapshot := historicalArchiveSnapshot(t, dataDir, recordingID)
		t.Fatalf("first historical recovery pass did not schedule a second pass: requests=%d media=%d archive_revision=%d states(seq2,128,129)=%s/%s/%s host=%s", len(fixture.historicalRequestsFor(stream)), len(snapshot.media), snapshot.header.ArchiveRevision, snapshot.coverageState(2), snapshot.coverageState(128), snapshot.coverageState(129), boundedOutput([]byte(process.output.String())))
	}
	passOne := historicalArchiveSnapshot(t, dataDir, recordingID)
	if !passOne.hasMedia(1) || passOne.coverageState(2) != archiveindex.CoverageKnownMissing || passOne.coverageState(128) != archiveindex.CoverageKnownMissing || passOne.coverageState(129) != archiveindex.CoverageUnknown {
		states := make([]string, 0, 10)
		for sequence := uint64(2); sequence <= 11; sequence++ {
			states = append(states, fmt.Sprintf("%d=%s", sequence, passOne.coverageState(sequence)))
		}
		coverageTail := passOne.coverage[max(0, len(passOne.coverage)-4):]
		t.Fatalf("first historical pass did not preserve the exact 128-work boundary: media1=%t gap2=%s gap128=%s gap129=%s coverage_observations=%d states=%v tail=%+v track_epoch=%d history_requests=%d host=%s children=%s", passOne.hasMedia(1), passOne.coverageState(2), passOne.coverageState(128), passOne.coverageState(129), len(passOne.coverage), states, coverageTail, passOne.header.Tracks["main"].SourceEpoch, len(fixture.historicalRequestsFor(stream)), boundedOutput([]byte(process.output.String())), boundedOutput([]byte(readRuntimeE2EChildDiagnostics(diagnosticDir))))
	}
	knownMissingWork := 0
	for sequence := uint64(2); sequence <= 128; sequence++ {
		if passOne.coverageState(sequence) == archiveindex.CoverageKnownMissing {
			knownMissingWork++
		}
	}
	if knownMissingWork != 127 || historicalCanonicalRevisionDelta(preRepairHeader, passOne.header) != 128 {
		t.Fatalf("first bounded pass did not publish one prefix plus 127 explicit gaps: known_missing=%d semantic_revision_delta=%d raw_revision=%d→%d manifest_snapshots=%d→%d", knownMissingWork, historicalCanonicalRevisionDelta(preRepairHeader, passOne.header), preRepairHeader.ArchiveRevision, passOne.header.ArchiveRevision, preRepairHeader.ShardedArchive.ManifestSnapshotCount, passOne.header.ShardedArchive.ManifestSnapshotCount)
	}
	t.Logf("first historical pass: work=128 media=1 known_missing=%d semantic_revision_delta=%d raw_revision=%d→%d manifest_snapshots=%d→%d", knownMissingWork, historicalCanonicalRevisionDelta(preRepairHeader, passOne.header), preRepairHeader.ArchiveRevision, passOne.header.ArchiveRevision, preRepairHeader.ShardedArchive.ManifestSnapshotCount, passOne.header.ShardedArchive.ManifestSnapshotCount)
	if len(passOne.media) != 3 {
		t.Fatalf("first historical pass canonical media count=%d, want one history prefix plus two live media", len(passOne.media))
	}

	releaseSecond()
	deadline := time.Now().Add(150 * time.Second)
	var passTwo historicalAcceptanceSnapshot
	for time.Now().Before(deadline) {
		passTwo = historicalArchiveSnapshot(t, dataDir, recordingID)
		if passTwo.coverageState(2) == archiveindex.CoveragePresent && passTwo.coverageState(129) == archiveindex.CoverageKnownMissing {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	if passTwo.coverageState(2) != archiveindex.CoveragePresent || !passTwo.hasMedia(2) || passTwo.coverageState(129) != archiveindex.CoverageKnownMissing {
		t.Fatalf("second historical pass did not repair gap 2 and record new gap 129: seq2=%s media2=%t seq129=%s requests=%d", passTwo.coverageState(2), passTwo.hasMedia(2), passTwo.coverageState(129), len(fixture.historicalRequestsFor(stream)))
	}
	for sequence := uint64(1); sequence <= 129; sequence++ {
		want := archiveindex.CoverageKnownMissing
		if sequence <= 2 {
			want = archiveindex.CoveragePresent
		}
		if got := passTwo.coverageState(sequence); got != want {
			t.Fatalf("historical logical coordinate %d state=%s, want %s", sequence, got, want)
		}
	}
	if passTwo.coverageState(130) != archiveindex.CoverageUnknown {
		t.Fatalf("historical fixture created an undeclared coordinate at 130: %s", passTwo.coverageState(130))
	}
	if historicalCanonicalRevisionDelta(passOne.header, passTwo.header) != 2 {
		t.Fatalf("second pass semantic archive revision delta=%d, want one gap→present commit plus one new known_missing coordinate; reobserved known_missing coordinates must be revision no-ops; raw_revision=%d→%d manifest_snapshots=%d→%d", historicalCanonicalRevisionDelta(passOne.header, passTwo.header), passOne.header.ArchiveRevision, passTwo.header.ArchiveRevision, passOne.header.ShardedArchive.ManifestSnapshotCount, passTwo.header.ShardedArchive.ManifestSnapshotCount)
	}
	if len(passTwo.media) != 4 {
		t.Fatalf("canonical media count=%d after repair, want prefix, repaired gap, and two live media", len(passTwo.media))
	}
	passTwo.assertNoDuplicateCoordinates(t)
	passTwo.assertStableExistingOrdinals(t, passOne)
	postRepairPlaylist := getHistoricalLivePlaylist(t, client, baseURL, recordingID)
	if livePlaylistMediaSequence(postRepairPlaylist) != livePlaylistMediaSequence(preRepairPlaylist) {
		t.Fatalf("historical repair changed active live MEDIA-SEQUENCE: before=%s after=%s", preRepairPlaylist, postRepairPlaylist)
	}
	preURIs, postURIs := livePlaylistURIs(preRepairPlaylist), livePlaylistURIs(postRepairPlaylist)
	if len(preURIs) != len(postURIs) {
		t.Fatalf("historical repair changed the active live playlist URI count: before=%v after=%v", preURIs, postURIs)
	}
	for i := range preURIs {
		if preURIs[i] != postURIs[i] {
			t.Fatalf("historical prefix/gap repair changed live URI identity at index %d: %q -> %q", i, preURIs[i], postURIs[i])
		}
	}
	if passTwo.header.TimelineRevision <= passOne.header.TimelineRevision {
		t.Fatalf("gap→present did not advance playback ordering revision: before=%d after=%d", passOne.header.TimelineRevision, passTwo.header.TimelineRevision)
	}
	t.Logf("actual Runtime Host historical repair: recording=%s passes=2 work/pass<=128 semantic_revision_delta=%d+%d media=%d live_uri_count=%d duplicate_coordinates=0", recordingID, historicalCanonicalRevisionDelta(preRepairHeader, passOne.header), historicalCanonicalRevisionDelta(passOne.header, passTwo.header), len(passTwo.media), len(postURIs))
}

func historicalCanonicalRevisionDelta(before, after *domain.Recording) uint64 {
	if before == nil || after == nil || before.ShardedArchive == nil || after.ShardedArchive == nil || after.ArchiveRevision < before.ArchiveRevision || after.ShardedArchive.ManifestSnapshotCount < before.ShardedArchive.ManifestSnapshotCount {
		return 0
	}
	rawDelta := after.ArchiveRevision - before.ArchiveRevision
	manifestDelta := after.ShardedArchive.ManifestSnapshotCount - before.ShardedArchive.ManifestSnapshotCount
	if rawDelta < manifestDelta {
		return 0
	}
	return rawDelta - manifestDelta
}

type historicalAcceptanceSnapshot struct {
	header     *domain.Recording
	media      []storage.V2MediaRecord
	coverage   []archiveindex.Coverage
	coordinate archiveindex.Coordinate
}

func historicalArchiveSnapshot(t *testing.T, dataDir, recordingID string) historicalAcceptanceSnapshot {
	t.Helper()
	store, err := storage.New(dataDir)
	if err != nil {
		t.Fatalf("open canonical V2 archive: %v", err)
	}
	header, err := store.LoadRecordingHeader(context.Background(), recordingID)
	if err != nil {
		t.Fatalf("load canonical V2 root: %v", err)
	}
	var snapshot historicalAcceptanceSnapshot
	snapshot.header = header
	if err := store.IterateShardedTimeline(context.Background(), recordingID, "main", func(record storage.V2MediaRecord) error {
		snapshot.media = append(snapshot.media, record)
		return nil
	}); err != nil {
		t.Fatalf("iterate canonical timeline: %v", err)
	}
	if err := store.IterateShardedCoverage(context.Background(), recordingID, func(value archiveindex.Coverage) error {
		snapshot.coverage = append(snapshot.coverage, value)
		return nil
	}); err != nil {
		t.Fatalf("iterate canonical coverage observations: %v", err)
	}
	track := header.Tracks["main"]
	if track == nil {
		t.Fatal("canonical V2 root has no main track")
	}
	snapshot.coordinate = archiveindex.Coordinate{
		SessionID: header.SourceSessionID, TrackID: "main", SourceEpoch: track.SourceEpoch,
		DiscontinuitySequence: 0, Kind: archiveindex.ObjectMedia,
	}
	return snapshot
}

func (s historicalAcceptanceSnapshot) coverageState(sequence uint64) archiveindex.CoverageState {
	coordinate := s.coordinate
	coordinate.Sequence = sequence
	inventory := archiveindex.Inventory{Coverage: s.coverage}
	for _, record := range s.media {
		inventory.Segments = append(inventory.Segments, archiveindex.Segment{Coordinate: record.Coordinate, State: archiveindex.CoveragePresent})
	}
	return archiveindex.CoverageAt(inventory, coordinate)
}

func (s historicalAcceptanceSnapshot) hasMedia(sequence uint64) bool {
	want := s.coordinate
	want.Sequence = sequence
	for _, record := range s.media {
		if record.Coordinate == want {
			return true
		}
	}
	return false
}

func (s historicalAcceptanceSnapshot) assertNoDuplicateCoordinates(t *testing.T) {
	t.Helper()
	coordinates := make(map[archiveindex.Coordinate]string, len(s.media))
	ids := make(map[string]struct{}, len(s.media))
	for _, record := range s.media {
		if prior, exists := coordinates[record.Coordinate]; exists {
			t.Fatalf("duplicate canonical coordinate committed: coordinate=%+v ids=%s,%s", record.Coordinate, prior, record.Segment.ID)
		}
		coordinates[record.Coordinate] = record.Segment.ID
		if _, exists := ids[record.Segment.ID]; exists {
			t.Fatalf("duplicate canonical media ID committed: %s", record.Segment.ID)
		}
		ids[record.Segment.ID] = struct{}{}
	}
}

func (s historicalAcceptanceSnapshot) assertStableExistingOrdinals(t *testing.T, prior historicalAcceptanceSnapshot) {
	t.Helper()
	priorByCoordinate := make(map[archiveindex.Coordinate]domain.Segment, len(prior.media))
	for _, record := range prior.media {
		priorByCoordinate[record.Coordinate] = record.Segment
	}
	for _, record := range s.media {
		before, exists := priorByCoordinate[record.Coordinate]
		if !exists {
			continue
		}
		if before.ArchiveOrdinal != record.Segment.ArchiveOrdinal || before.LivePresentationOrdinal != record.Segment.LivePresentationOrdinal {
			t.Fatalf("historical repair renumbered existing archive/live identity at %+v: before archive/live=%d/%d after=%d/%d", record.Coordinate, before.ArchiveOrdinal, before.LivePresentationOrdinal, record.Segment.ArchiveOrdinal, record.Segment.LivePresentationOrdinal)
		}
	}
}

func historicalRepairManifest(origin, stream string, last uint64, sequenceTwoPresent bool) []byte {
	var body strings.Builder
	fmt.Fprintf(&body, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:1\n#EXT-X-DISCONTINUITY-SEQUENCE:0\n")
	for sequence := uint64(1); sequence <= last; sequence++ {
		if sequence != 1 && !(sequence == 2 && sequenceTwoPresent) {
			body.WriteString("#EXT-X-GAP\n")
		}
		fmt.Fprintf(&body, "#EXTINF:1.0,\n%s/hls/segments/%06d.ts?stream=%s&token=token-0\n", origin, sequence, urlQueryEscape(stream))
	}
	body.WriteString("#EXT-X-ENDLIST\n")
	return []byte(body.String())
}

func loadHistoricalHeader(t *testing.T, dataDir, recordingID string) *domain.Recording {
	t.Helper()
	store, err := storage.New(dataDir)
	if err != nil {
		t.Fatalf("open archive while recording live: %v", err)
	}
	header, err := store.LoadRecordingHeader(context.Background(), recordingID)
	if err != nil {
		t.Fatalf("load archive header while recording live: %v", err)
	}
	return header
}

func getHistoricalLivePlaylist(t *testing.T, client *http.Client, baseURL, recordingID string) string {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, baseURL+"/api/recordings/"+recordingID+"/play/live/tracks/main/playlist.m3u8", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("read active live playlist: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read active live playlist body: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("active live playlist status=%d body=%s", response.StatusCode, boundedOutput(body))
	}
	return string(body)
}

func livePlaylistMediaSequence(playlist string) uint64 {
	for _, line := range strings.Split(playlist, "\n") {
		if value, found := strings.CutPrefix(strings.TrimSpace(line), "#EXT-X-MEDIA-SEQUENCE:"); found {
			var sequence uint64
			_, _ = fmt.Sscanf(value, "%d", &sequence)
			return sequence
		}
	}
	return 0
}

func livePlaylistURIs(playlist string) []string {
	var values []string
	for _, line := range strings.Split(playlist, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			values = append(values, line)
		}
	}
	return values
}
