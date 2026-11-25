//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package archivecatalog

import (
	"os"
	"syscall"
)

func catalogFlock(file *os.File) error { return syscall.Flock(int(file.Fd()), syscall.LOCK_EX) }
func catalogFunlock(file *os.File)     { _ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN) }
