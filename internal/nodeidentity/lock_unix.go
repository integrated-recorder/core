//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package nodeidentity

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

func lockNodeIdentity(dataRoot string) (func(), error) {
	path := filepath.Join(dataRoot, "security", "node-key.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.New("node identity lock is unavailable")
	}
	info, statErr := os.Lstat(path)
	opened, fileErr := file.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, opened) || info.Mode().Perm() != 0600 {
		_ = file.Close()
		return nil, ErrCorrupt
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, errors.New("node identity lock could not be acquired")
	}
	return func() {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
	}, nil
}
