package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type SetupProbeResult struct {
	FreeBytes        uint64
	CapacityKnown    bool
	WritePassed      bool
	DurabilityPassed bool
}

const maxStaleSetupProbeEntries = 128

// cleanupSetupProbeResidue removes only directories created by a previous
// interrupted setup probe. It is deliberately best-effort and bounded so
// diagnostic cleanup cannot prevent archive startup. Symlinks and other file
// types are never followed or removed.
func cleanupSetupProbeResidue(stateDir string) {
	info, err := os.Lstat(stateDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return
	}
	dir, err := os.Open(stateDir)
	if err != nil {
		return
	}
	defer dir.Close()
	remaining := maxStaleSetupProbeEntries
	removed := false
	defer func() {
		if removed {
			_ = dir.Sync()
		}
	}()
	for remaining > 0 {
		entries, readErr := dir.ReadDir(remaining)
		if readErr != nil && len(entries) == 0 {
			return
		}
		if len(entries) == 0 {
			return
		}
		for _, entry := range entries {
			remaining--
			if !strings.HasPrefix(entry.Name(), ".setup-probe-") || len(entry.Name()) > 128 {
				continue
			}
			entryInfo, statErr := os.Lstat(filepath.Join(stateDir, entry.Name()))
			if statErr != nil || !entryInfo.IsDir() || entryInfo.Mode()&os.ModeSymlink != 0 {
				continue
			}
			if os.RemoveAll(filepath.Join(stateDir, entry.Name())) == nil {
				removed = true
			}
		}
		if readErr != nil {
			return
		}
	}
}

// RunSetupProbe performs a bounded, disposable write/read/sync/delete check
// in Runtime Host's private diagnostic area. It never creates or modifies a
// canonical recording or management record.
func (s *Store) RunSetupProbe() SetupProbeResult {
	if s == nil || s.StorageBackend == nil {
		return SetupProbeResult{}
	}
	switch backend := s.StorageBackend.(type) {
	case *ObjectStoreArchiveBackend:
		if backend == nil || backend.objects == nil {
			return SetupProbeResult{}
		}
		// The provider protocol owns physical publication. Its probe exercises
		// atomic Put, read-back, range, listing, and deletion in a reserved
		// non-archive namespace. Capacity is intentionally unknown unless the
		// provider exposes a reliable capacity contract.
		if err := ProbePhysicalObjectStore(context.Background(), backend.objects); err != nil {
			return SetupProbeResult{}
		}
		return SetupProbeResult{WritePassed: true, DurabilityPassed: true}
	case *LocalFilesystemBackend:
		if backend == nil {
			return SetupProbeResult{}
		}
		return backend.runSetupProbeResult()
	default:
		return SetupProbeResult{}
	}
}

func (b *LocalFilesystemBackend) runSetupProbeResult() SetupProbeResult {
	if b == nil {
		return SetupProbeResult{}
	}
	b.setupProbeMu.Lock()
	defer b.setupProbeMu.Unlock()
	var fs syscall.Statfs_t
	if err := syscall.Statfs(b.root, &fs); err != nil || fs.Bsize <= 0 {
		return SetupProbeResult{}
	}
	capacity, valid := capacityFromBlocks(uint64(fs.Blocks), uint64(fs.Bfree), uint64(fs.Bavail), uint64(fs.Bsize))
	if !valid {
		return SetupProbeResult{}
	}
	result := SetupProbeResult{FreeBytes: capacity.AvailableBytes, CapacityKnown: true}
	writePassed, durabilityPassed := b.runSetupProbe()
	result.WritePassed = writePassed
	result.DurabilityPassed = durabilityPassed
	return result
}

func (b *LocalFilesystemBackend) runSetupProbe() (writePassed, durabilityPassed bool) {
	if b == nil || !filepath.IsAbs(b.root) {
		return false, false
	}
	for _, path := range []string{filepath.Join(b.root, "recordings"), filepath.Join(b.root, "runtime"), filepath.Join(b.root, "runtime", "state")} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return false, false
		}
	}
	recordingsDir, err := os.Open(filepath.Join(b.root, "recordings"))
	if err != nil {
		return false, false
	}
	_, readDirErr := recordingsDir.ReadDir(1)
	closeErr := recordingsDir.Close()
	if readDirErr != nil && readDirErr != io.EOF || closeErr != nil {
		return false, false
	}
	diagnosticRoot := filepath.Join(b.root, "runtime", "state")
	probeDir, err := os.MkdirTemp(diagnosticRoot, ".setup-probe-")
	if err != nil {
		return false, false
	}
	if err := os.Chmod(probeDir, 0700); err != nil {
		_ = os.RemoveAll(probeDir)
		return false, false
	}
	defer func() {
		if err := os.RemoveAll(probeDir); err != nil {
			writePassed, durabilityPassed = false, false
		}
		if err := syncDiagnosticDirectory(diagnosticRoot); err != nil {
			writePassed, durabilityPassed = false, false
		}
	}()
	var expected [1024]byte
	if _, err := rand.Read(expected[:]); err != nil {
		return false, false
	}
	path := filepath.Join(probeDir, "probe")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return false, false
	}
	_, writeErr := f.Write(expected[:])
	syncErr := f.Sync()
	closeErr = f.Close()
	writePassed = writeErr == nil && syncErr == nil && closeErr == nil
	if !writePassed {
		return writePassed, false
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 {
		return true, false
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, expected[:]) {
		return true, false
	}
	dirErr := syncDiagnosticDirectory(probeDir)
	// A second independent open verifies the published bytes and the directory
	// entry after both file and directory durability barriers have completed.
	reader, err := os.Open(path)
	if err != nil {
		return true, false
	}
	verified, readErr := io.ReadAll(io.LimitReader(reader, int64(len(expected))+1))
	reader.Close()
	durabilityPassed = dirErr == nil && readErr == nil && bytes.Equal(verified, expected[:])
	if removeErr := os.Remove(path); removeErr != nil {
		durabilityPassed = false
	}
	if syncErr := syncDiagnosticDirectory(probeDir); syncErr != nil {
		durabilityPassed = false
	}
	return writePassed, durabilityPassed
}

func syncDiagnosticDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return err
	}
	return nil
}
