//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package lease

import (
	"os"
	"syscall"
)

func leaseLock(file *os.File) error { return syscall.Flock(int(file.Fd()), syscall.LOCK_EX) }
func leaseUnlock(file *os.File)     { _ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN) }
