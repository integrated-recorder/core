//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package authn

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockAuthStoreFile coordinates independent Runtime Host and Control
// processes that share DATA_DIR. The lock file is persistent; flock ownership
// is released by the kernel after process exit, including crashes.
func lockAuthStoreFile(securityDir string) (func(), error) {
	path := filepath.Join(securityDir, "auth-store.lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("%w: open auth store lock", ErrStorage)
	}
	info, statErr := os.Lstat(path)
	opened, fileErr := f.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, opened) {
		_ = f.Close()
		return nil, ErrCorruptStore
	}
	if err := f.Chmod(0600); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%w: secure auth store lock", ErrStorage)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%w: acquire auth store lock", ErrStorage)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
