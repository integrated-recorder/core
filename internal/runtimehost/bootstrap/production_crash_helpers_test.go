package bootstrap

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/runtimehook"
)

func writeRuntimeHookArm(directory string, arm runtimehook.Arm) error {
	encoded, err := json.Marshal(arm)
	if err != nil || len(encoded) > 512 {
		return errors.New("failpoint arm is invalid")
	}
	temp, err := os.CreateTemp(directory, ".arm-*.tmp")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(encoded); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempName, runtimehook.ArmPath(directory)); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func writeRuntimeHookRelease(directory string, point runtimehook.Point, recordingID string) error {
	path := runtimehook.ReleaseMarkerPath(directory, point, recordingID)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func pointNeedsPreparedContinuation(point runtimehook.Point) bool {
	switch point {
	case runtimehook.AfterTargetReady, runtimehook.BeforeOwnerCAS,
		runtimehook.AfterOwnerCAS, runtimehook.BeforeTargetActivation,
		runtimehook.BeforeTargetFirstCommit, runtimehook.AfterTargetFirstCommit,
		runtimehook.BeforeSourceRetirement, runtimehook.DuringGenerationLeaseReconcile:
		return true
	default:
		return false
	}
}

func pointAfterOwnerCAS(point runtimehook.Point) bool {
	switch point {
	case runtimehook.AfterOwnerCAS, runtimehook.BeforeTargetActivation,
		runtimehook.BeforeTargetFirstCommit, runtimehook.AfterTargetFirstCommit,
		runtimehook.BeforeSourceRetirement, runtimehook.DuringGenerationLeaseReconcile:
		return true
	default:
		return false
	}
}

func snapshotRecordingObjects(root string) (map[string]string, error) {
	objects := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("canonical archive contains symlink %q", filepath.Base(path))
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("canonical archive contains non-regular object %q", filepath.Base(path))
		}
		if strings.HasPrefix(filepath.Base(path), "\x1f.ir-storage-local-tmp-") {
			// These are incomplete atomic-Put staging objects, not canonical logical objects.
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		// Cold recovery is allowed to publish the explicit Interrupted state in
		// the root document. All payload, sidecar, manifest and metadata objects
		// must remain byte-for-byte unchanged.
		if filepath.Clean(relative) == "recording.json" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(data)
		objects[filepath.ToSlash(relative)] = hex.EncodeToString(digest[:])
		return nil
	})
	return objects, err
}

// compareArchiveSnapshotsAllowingManifestAppend proves that the durable
// pre-crash archive remains byte-for-byte intact while allowing only
// append-only source manifest objects published during recovery. SaveSnapshot
// publishes the manifest payload before its sidecar; a crash between those
// atomic object writes can leave an unreferenced manifest payload. Storage
// recovery deliberately reports and preserves such orphan payloads without
// attaching them to the recording.
func compareArchiveSnapshotsAllowingManifestAppend(before, after map[string]string) error {
	for object, digest := range before {
		actual, ok := after[object]
		if !ok {
			return fmt.Errorf("pre-crash archive object %q was removed", object)
		}
		if actual != digest {
			return fmt.Errorf("pre-crash archive object %q changed digest", object)
		}
	}

	added := make(map[string]struct{})
	for object := range after {
		if _, existed := before[object]; !existed {
			added[object] = struct{}{}
		}
	}
	for object := range added {
		partner, ok := manifestSnapshotPartner(object)
		if !ok {
			return fmt.Errorf("unexpected archive object addition %q", object)
		}
		if strings.HasSuffix(object, ".json") {
			if _, exists := after[partner]; !exists {
				return fmt.Errorf("new manifest snapshot %q has no matching pair %q", object, partner)
			}
		}
	}
	return nil
}

// compareArchiveSnapshotsAllowingV2ManifestAppend permits bounded V2 manifest
// page replacement only when the decoded canonical manifest history is a
// strict append. V2's current final page is an atomic bounded page, so adding
// a snapshot may replace that page while preserving all prior entries.
func compareArchiveSnapshotsAllowingV2ManifestAppend(root string, before, after map[string]string, beforeSnapshots, afterSnapshots []domain.ManifestSnapshot) error {
	if len(afterSnapshots) < len(beforeSnapshots) {
		return fmt.Errorf("V2 manifest history shrank from %d to %d entries", len(beforeSnapshots), len(afterSnapshots))
	}
	for index := range beforeSnapshots {
		if !equalManifestSnapshot(beforeSnapshots[index], afterSnapshots[index]) {
			oldSnapshot, newSnapshot := beforeSnapshots[index], afterSnapshots[index]
			beforeSource := sha256.Sum256([]byte(oldSnapshot.SourceURI))
			afterSource := sha256.Sum256([]byte(newSnapshot.SourceURI))
			pageName := fmt.Sprintf("archive/v2/manifests/%020d.json", index/64)
			return fmt.Errorf("V2 manifest history changed existing entry %d/%d: path=%t track=%t source_uri_sha256=%x->%x time_equal=%t digest=%t size=%t page_digest_same=%t", index, len(beforeSnapshots), oldSnapshot.StoragePath == newSnapshot.StoragePath, oldSnapshot.TrackID == newSnapshot.TrackID, beforeSource[:8], afterSource[:8], oldSnapshot.FetchedAt.Equal(newSnapshot.FetchedAt), oldSnapshot.SHA256 == newSnapshot.SHA256, oldSnapshot.Size == newSnapshot.Size, before[pageName] == after[pageName])
		}
	}

	filteredBefore := make(map[string]string, len(before))
	for object, digest := range before {
		if isV2ManifestPagePath(object) {
			current, exists := after[object]
			if !exists {
				return fmt.Errorf("pre-crash V2 manifest page %q was removed", object)
			}
			if current != digest {
				page, _ := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(object, "archive/v2/manifests/"), ".json"), 10, 64)
				lastBeforePage := uint64(0)
				canAppendLastPage := len(beforeSnapshots) > 0 && len(beforeSnapshots)%64 != 0 && len(afterSnapshots) > len(beforeSnapshots)
				if canAppendLastPage {
					lastBeforePage = uint64((len(beforeSnapshots) - 1) / 64)
				}
				if !canAppendLastPage || page != lastBeforePage {
					return fmt.Errorf("V2 manifest page %q changed outside an append to the partial final page", object)
				}
			}
			continue
		}
		filteredBefore[object] = digest
	}
	filteredAfter := make(map[string]string, len(after))
	for object, digest := range after {
		if isV2ManifestPagePath(object) {
			continue
		}
		if strings.HasPrefix(object, "archive/v2/manifests/") {
			return fmt.Errorf("invalid V2 manifest page path %q", object)
		}
		filteredAfter[object] = digest
	}
	if err := compareArchiveSnapshotsAllowingManifestAppend(filteredBefore, filteredAfter); err != nil {
		return err
	}
	return validateV2ManifestPages(root, after, afterSnapshots)
}

func equalManifestSnapshot(left, right domain.ManifestSnapshot) bool {
	return left.TrackID == right.TrackID && left.SourceURI == right.SourceURI && left.StoragePath == right.StoragePath && left.FetchedAt.Equal(right.FetchedAt) && left.SHA256 == right.SHA256 && left.Size == right.Size
}

func isV2ManifestPagePath(object string) bool {
	const prefix = "archive/v2/manifests/"
	if !strings.HasPrefix(object, prefix) || !strings.HasSuffix(object, ".json") {
		return false
	}
	page := strings.TrimSuffix(strings.TrimPrefix(object, prefix), ".json")
	if len(page) != 20 {
		return false
	}
	_, err := strconv.ParseUint(page, 10, 64)
	return err == nil
}

func validateV2ManifestPages(root string, objects map[string]string, snapshots []domain.ManifestSnapshot) error {
	const pageEntries = 64
	pageCount := (len(snapshots) + pageEntries - 1) / pageEntries
	for pageNumber := 0; pageNumber < pageCount; pageNumber++ {
		name := fmt.Sprintf("archive/v2/manifests/%020d.json", pageNumber)
		if _, exists := objects[name]; !exists {
			return fmt.Errorf("V2 manifest page %q is missing", name)
		}
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
		if err != nil {
			return fmt.Errorf("read V2 manifest page %q: %w", name, err)
		}
		digest := sha256.Sum256(data)
		if hex.EncodeToString(digest[:]) != objects[name] {
			return fmt.Errorf("V2 manifest page %q digest does not match its snapshot", name)
		}
		var page struct {
			Version int                       `json:"version"`
			Number  uint64                    `json:"number"`
			Entries []domain.ManifestSnapshot `json:"entries"`
		}
		if err := json.Unmarshal(data, &page); err != nil {
			return fmt.Errorf("decode V2 manifest page %q: %w", name, err)
		}
		start := pageNumber * pageEntries
		end := start + pageEntries
		if end > len(snapshots) {
			end = len(snapshots)
		}
		if page.Version != 1 || page.Number != uint64(pageNumber) || !reflect.DeepEqual(page.Entries, snapshots[start:end]) {
			return fmt.Errorf("V2 manifest page %q does not match append-only canonical history", name)
		}
	}
	for object := range objects {
		if strings.HasPrefix(object, "archive/v2/manifests/") {
			if !isV2ManifestPagePath(object) {
				return fmt.Errorf("invalid V2 manifest page path %q", object)
			}
			pageName := strings.TrimSuffix(strings.TrimPrefix(object, "archive/v2/manifests/"), ".json")
			pageNumber, _ := strconv.ParseUint(pageName, 10, 64)
			if pageNumber >= uint64(pageCount) {
				return fmt.Errorf("unreferenced V2 manifest page %q", object)
			}
		}
	}
	return nil
}

func v2ManifestPageSnapshotsEqual(left, right map[string]string) bool {
	const prefix = "archive/v2/manifests/"
	for object, digest := range left {
		if strings.HasPrefix(object, prefix) && right[object] != digest {
			return false
		}
	}
	for object, digest := range right {
		if strings.HasPrefix(object, prefix) && left[object] != digest {
			return false
		}
	}
	return true
}

func validateManifestSnapshotReferences(recording *domain.Recording, objects map[string]string) error {
	if recording == nil {
		return errors.New("recording is unavailable while validating manifest references")
	}
	for _, snapshot := range recording.Snapshots {
		if _, exists := objects[snapshot.StoragePath]; !exists {
			return fmt.Errorf("recording references missing manifest payload %q", snapshot.StoragePath)
		}
		if _, exists := objects[snapshot.StoragePath+".json"]; !exists {
			return fmt.Errorf("recording references manifest payload %q without its committed sidecar", snapshot.StoragePath)
		}
	}
	return nil
}

func manifestSnapshotPartner(object string) (string, bool) {
	const prefix = "manifests/main-"
	if !strings.HasPrefix(object, prefix) {
		return "", false
	}
	remainder := strings.TrimPrefix(object, prefix)
	if remainder == "" || strings.Contains(remainder, "/") {
		return "", false
	}
	switch {
	case strings.HasSuffix(remainder, ".m3u8.json"):
		stem := strings.TrimSuffix(remainder, ".m3u8.json")
		if stem == "" {
			return "", false
		}
		return strings.TrimSuffix(object, ".json"), true
	case strings.HasSuffix(remainder, ".m3u8"):
		stem := strings.TrimSuffix(remainder, ".m3u8")
		if stem == "" {
			return "", false
		}
		return object + ".json", true
	default:
		return "", false
	}
}

func readRuntimeE2EChildDiagnostics(directory string) string {
	if directory == "" {
		return ""
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return ""
	}
	var result strings.Builder
	remaining := 12 << 10
	var names []string
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".child-diagnostic-") || !strings.HasSuffix(entry.Name(), ".log") || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() < 0 || info.Size() > 32<<10 {
			continue
		}
		names = append(names, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil || len(data) == 0 {
			continue
		}
		if len(data) > remaining {
			data = data[:remaining]
		}
		result.WriteString("\nchild diagnostic: ")
		result.Write(data)
		remaining -= len(data)
		if remaining <= 0 {
			break
		}
	}
	if result.Len() == 0 && len(names) == 0 {
		return ""
	}
	if result.Len() == 0 {
		result.WriteString("\nchild diagnostic files were empty: ")
		result.WriteString(strings.Join(names, ","))
	}
	return result.String()
}

func hardKillRuntimeHost(process *runtimeHostProcess, timeout time.Duration) error {
	if process == nil || process.command == nil || process.command.Process == nil {
		return errors.New("Runtime Host process is unavailable")
	}
	if err := process.command.Process.Signal(syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	select {
	case <-process.done:
		return nil
	case <-time.After(timeout):
		return errors.New("SIGKILLed Runtime Host did not exit within the bound")
	}
}

func processIDsForBinary(path string) []int {
	processes, err := runtimeProcessTable()
	if err != nil {
		return nil
	}
	clean := filepath.Clean(path)
	var result []int
	for _, process := range processes {
		if process.PID != os.Getpid() && commandRunsBinary(process.Command, clean) {
			result = append(result, process.PID)
		}
	}
	return result
}

func commandRunsBinary(command, binaryPath string) bool {
	fields := strings.Fields(command)
	if len(fields) == 0 {
		return false
	}
	return filepath.Clean(strings.Trim(fields[0], `"'`)) == filepath.Clean(binaryPath)
}

func processPIDPresent(pid int) bool {
	processes, err := runtimeProcessTable()
	if err != nil {
		return false
	}
	for _, process := range processes {
		if process.PID == pid {
			return true
		}
	}
	return false
}

func terminateRecordedProcesses(recorded map[int]string, timeout time.Duration) error {
	if len(recorded) == 0 {
		return nil
	}
	send := func(signal syscall.Signal) []int {
		var alive []int
		processes, err := runtimeProcessTable()
		if err != nil {
			for pid := range recorded {
				alive = append(alive, pid)
			}
			return alive
		}
		byPID := make(map[int]string, len(processes))
		for _, process := range processes {
			byPID[process.PID] = process.Command
		}
		for pid, binary := range recorded {
			command, exists := byPID[pid]
			if !exists || !commandRunsBinary(command, binary) {
				continue
			}
			alive = append(alive, pid)
			_ = syscall.Kill(pid, signal)
		}
		return alive
	}

	remaining := send(syscall.SIGTERM)
	deadline := time.Now().Add(timeout)
	for len(remaining) > 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		remaining = send(syscall.Signal(0))
	}
	if len(remaining) > 0 {
		remaining = send(syscall.SIGKILL)
		killDeadline := time.Now().Add(5 * time.Second)
		for len(remaining) > 0 && time.Now().Before(killDeadline) {
			time.Sleep(50 * time.Millisecond)
			remaining = send(syscall.Signal(0))
		}
	}
	if len(remaining) > 0 {
		return fmt.Errorf("recorded product process PIDs remain after bounded cleanup: %v", remaining)
	}
	return nil
}
