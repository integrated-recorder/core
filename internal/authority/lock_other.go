//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package authority

import (
	"os"
	"sync"
)

var authorityProcessLock sync.Mutex

// Non-Unix builds serialize within a process. Supported native and container
// targets use flock-backed cross-process serialization.
func authorityFlock(*os.File) error {
	authorityProcessLock.Lock()
	return nil
}

func authorityFunlock(*os.File) { authorityProcessLock.Unlock() }
