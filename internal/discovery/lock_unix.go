//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package discovery

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

func lockDirectory(root string) (func(), error) {
	path := filepath.Join(root, ".directory.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, directoryFileMode)
	if err != nil {
		return nil, errors.New("discovery directory lock is unavailable")
	}
	info, statErr := os.Lstat(path)
	opened, fileErr := file.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, opened) || info.Mode().Perm() != directoryFileMode {
		_ = file.Close()
		return nil, ErrDirectoryInvalid
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, errors.New("discovery directory lock could not be acquired")
	}
	return func() { _ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN); _ = file.Close() }, nil
}
