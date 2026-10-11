package storage

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	scaleAcceptanceTempPrefix = "integrated-recorder-scale-acceptance-"
	scaleAcceptanceTempMarker = ".ir-scale-acceptance-owner.json"
)

var scaleAcceptanceTempGrace = 5 * time.Minute

type scaleAcceptanceTempOwner struct {
	Version     int   `json:"version"`
	PID         int   `json:"pid"`
	CreatedAtMS int64 `json:"created_at_ms"`
}

func newScaleAcceptanceTempDir(t *testing.T) string {
	t.Helper()
	root, err := createScaleAcceptanceTempDir(os.TempDir(), os.Getpid(), time.Now())
	if err != nil {
		t.Fatalf("create scale acceptance temporary directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func createScaleAcceptanceTempDir(parent string, pid int, now time.Time) (string, error) {
	if pid < 1 {
		return "", errors.New("invalid scale temp owner")
	}
	if err := cleanupStaleScaleAcceptanceTempDirs(parent, now, func(ownerPID int) bool {
		return scaleAcceptanceProcessAlive(ownerPID)
	}); err != nil {
		return "", err
	}
	root, err := os.MkdirTemp(parent, scaleAcceptanceTempPrefix)
	if err != nil {
		return "", err
	}
	if err := os.Chmod(root, 0700); err != nil {
		_ = os.RemoveAll(root)
		return "", err
	}
	owner, err := json.Marshal(scaleAcceptanceTempOwner{Version: 1, PID: pid, CreatedAtMS: now.UnixMilli()})
	if err == nil {
		err = os.WriteFile(filepath.Join(root, scaleAcceptanceTempMarker), owner, 0600)
	}
	if err != nil {
		_ = os.RemoveAll(root)
		return "", err
	}
	return root, nil
}

func cleanupStaleScaleAcceptanceTempDirs(parent string, now time.Time, isAlive func(int) bool) error {
	root, err := filepath.Abs(parent)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), scaleAcceptanceTempPrefix) {
			continue
		}
		path := filepath.Join(root, entry.Name())
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !scaleAcceptanceTempOwnedByCurrentUser(info) {
			continue
		}
		markerPath := filepath.Join(path, scaleAcceptanceTempMarker)
		markerInfo, err := os.Lstat(markerPath)
		if err != nil || !markerInfo.Mode().IsRegular() || markerInfo.Mode()&os.ModeSymlink != 0 || !scaleAcceptanceTempOwnedByCurrentUser(markerInfo) {
			continue
		}
		data, err := os.ReadFile(markerPath)
		if err != nil {
			continue
		}
		var owner scaleAcceptanceTempOwner
		if json.Unmarshal(data, &owner) != nil || owner.Version != 1 || owner.PID < 1 || owner.CreatedAtMS < 1 {
			continue
		}
		created := time.UnixMilli(owner.CreatedAtMS)
		if now.Sub(created) < scaleAcceptanceTempGrace || isAlive(owner.PID) {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			return errors.New("stale scale acceptance temporary directory cleanup failed")
		}
	}
	return nil
}

func scaleAcceptanceTempOwnedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == os.Geteuid()
}

func scaleAcceptanceProcessAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func TestScaleAcceptanceTempCleanupReclaimsOnlyMarkedDeadOwners(t *testing.T) {
	root := t.TempDir()
	now := time.Unix(1_800_000_000, 0)
	old := filepath.Join(root, scaleAcceptanceTempPrefix+"old")
	active := filepath.Join(root, scaleAcceptanceTempPrefix+"active")
	recent := filepath.Join(root, scaleAcceptanceTempPrefix+"recent")
	unmarked := filepath.Join(root, scaleAcceptanceTempPrefix+"unmarked")
	badMarker := filepath.Join(root, scaleAcceptanceTempPrefix+"bad-marker")
	markerLink := filepath.Join(root, scaleAcceptanceTempPrefix+"marker-link")
	for _, path := range []string{old, active, recent, unmarked, badMarker} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	writeOwner := func(path string, pid int, created time.Time) {
		t.Helper()
		data, _ := json.Marshal(scaleAcceptanceTempOwner{Version: 1, PID: pid, CreatedAtMS: created.UnixMilli()})
		if err := os.WriteFile(filepath.Join(path, scaleAcceptanceTempMarker), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	writeOwner(old, 20_000_001, now.Add(-time.Hour))
	writeOwner(active, 20_000_002, now.Add(-time.Hour))
	writeOwner(recent, 20_000_003, now)
	if err := os.WriteFile(filepath.Join(badMarker, scaleAcceptanceTempMarker), []byte(`{"version":9,"pid":20000004,"created_at_ms":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(markerLink, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(badMarker, scaleAcceptanceTempMarker), filepath.Join(markerLink, scaleAcceptanceTempMarker)); err != nil {
		t.Fatal(err)
	}
	symlinkDir := filepath.Join(root, scaleAcceptanceTempPrefix+"symlink")
	if err := os.Symlink(badMarker, symlinkDir); err != nil {
		t.Fatal(err)
	}
	if err := cleanupStaleScaleAcceptanceTempDirs(root, now, func(pid int) bool { return pid == 20_000_002 }); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale owned directory remains: %v", err)
	}
	for _, path := range []string{active, recent, unmarked, badMarker} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("preserved directory %q: %v", filepath.Base(path), err)
		}
	}
	if info, err := os.Lstat(markerLink); err != nil || !info.IsDir() {
		t.Fatalf("preserved marker-link directory: info=%v err=%v", info, err)
	}
	if info, err := os.Lstat(symlinkDir); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("preserved symlink candidate: info=%v err=%v", info, err)
	}
}

func TestScaleAcceptanceTempNextInvocationReclaimsHardKilledOwner(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	stale := filepath.Join(root, scaleAcceptanceTempPrefix+"hard-killed")
	if err := os.Mkdir(stale, 0700); err != nil {
		t.Fatal(err)
	}
	owner, err := json.Marshal(scaleAcceptanceTempOwner{Version: 1, PID: 2_000_000_000, CreatedAtMS: now.Add(-time.Hour).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, scaleAcceptanceTempMarker), owner, 0600); err != nil {
		t.Fatal(err)
	}
	created, err := createScaleAcceptanceTempDir(root, os.Getpid(), now)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(created)
	if _, err := os.Lstat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("next fixture invocation did not reclaim stale owner: %v", err)
	}
}

func TestScaleAcceptanceTempCleanupKeepsRepeatedBaselinesFlat(t *testing.T) {
	root := t.TempDir()
	for cycle := 0; cycle < 100; cycle++ {
		path, err := os.MkdirTemp(root, scaleAcceptanceTempPrefix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "fixture"), make([]byte, 4096), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) != 0 {
			t.Fatalf("cycle %d left temporary entries=%d err=%v", cycle, len(entries), err)
		}
	}
}

func TestScaleAcceptanceTempNextInvocationReclaimsStaleAndRepeatedRunsStayFlat(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	stale := filepath.Join(root, scaleAcceptanceTempPrefix+"restart-orphan")
	if err := os.Mkdir(stale, 0700); err != nil {
		t.Fatal(err)
	}
	owner, err := json.Marshal(scaleAcceptanceTempOwner{Version: 1, PID: 2_000_000_000, CreatedAtMS: now.Add(-time.Hour).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, scaleAcceptanceTempMarker), owner, 0600); err != nil {
		t.Fatal(err)
	}
	created, err := createScaleAcceptanceTempDir(root, os.Getpid(), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("next fixture invocation did not reclaim stale owner: %v", err)
	}
	if err := os.RemoveAll(created); err != nil {
		t.Fatal(err)
	}

	baselineBytes := scaleTempTreeBytes(t, root)
	peakBytes := baselineBytes
	for cycle := 0; cycle < 10; cycle++ {
		path, err := createScaleAcceptanceTempDir(root, os.Getpid(), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "fixture.bin"), make([]byte, 512*1024), 0600); err != nil {
			t.Fatal(err)
		}
		if current := scaleTempTreeBytes(t, root); current > peakBytes {
			peakBytes = current
		}
		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
	}
	afterBytes := scaleTempTreeBytes(t, root)
	t.Logf("isolated scale scratch bytes: baseline=%d peak=%d after=%d cycles=10", baselineBytes, peakBytes, afterBytes)
	if afterBytes != baselineBytes || peakBytes <= baselineBytes {
		t.Fatalf("scale scratch baseline changed: baseline=%d peak=%d after=%d", baselineBytes, peakBytes, afterBytes)
	}
}

func scaleTempTreeBytes(t *testing.T, root string) int64 {
	t.Helper()
	var total int64
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		info, err := os.Lstat(filepath.Join(root, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if info.IsDir() {
			total += scaleTempTreeBytes(t, filepath.Join(root, entry.Name()))
		} else {
			total += info.Size()
		}
	}
	return total
}
