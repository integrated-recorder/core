//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package management

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func lockAuditFile(root string) (func(), error) {
	path := filepath.Join(root, "audit.lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("audit store is unavailable")
	}
	info, statErr := os.Lstat(path)
	opened, fileErr := f.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, opened) {
		_ = f.Close()
		return nil, fmt.Errorf("audit store is unsafe")
	}
	if err := f.Chmod(0600); err != nil || syscall.Flock(int(f.Fd()), syscall.LOCK_EX) != nil {
		_ = f.Close()
		return nil, fmt.Errorf("audit store is unavailable")
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
