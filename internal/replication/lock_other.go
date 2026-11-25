//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package replication

import (
	"errors"
	"os"
)

func replicationFlock(*os.File) error {
	return errors.New("replication storage locking is unsupported")
}
func replicationFunlock(*os.File)          {}
func replicationLockWouldBlock(error) bool { return false }
