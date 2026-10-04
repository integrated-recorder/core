package bootstrap

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

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
// pre-crash archive remains byte-for-byte intact while allowing complete
// append-only source manifest snapshots published by the still-authoritative
// source Engine during cold recovery.
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
		if _, exists := added[partner]; !exists {
			return fmt.Errorf("new manifest snapshot %q has no matching pair %q", object, partner)
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
