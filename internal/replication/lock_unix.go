//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package replication

import (
	"errors"
	"os"
	"syscall"
)

func replicationFlock(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}
func replicationFunlock(file *os.File) { _ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN) }
func replicationLockWouldBlock(err error) bool {
	return errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)
}
