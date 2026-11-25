//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package authority

import (
	"os"
	"syscall"
)

func authorityFlock(file *os.File) error { return syscall.Flock(int(file.Fd()), syscall.LOCK_EX) }
func authorityFunlock(file *os.File)     { _ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN) }
