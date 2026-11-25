//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package lease

import (
	"os"
	"sync"
)

var leaseProcessLock sync.Mutex

func leaseLock(*os.File) error { leaseProcessLock.Lock(); return nil }
func leaseUnlock(*os.File)     { leaseProcessLock.Unlock() }
