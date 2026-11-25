//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package globalidentity

import "sync"

var processIdentityLock sync.Mutex

// The product's supported native and container targets use the flock-backed
// implementation. This fallback keeps secondary Go targets buildable and
// serializes callers within one process.
func lockGlobalIdentity(string) (func(), error) {
	processIdentityLock.Lock()
	return processIdentityLock.Unlock, nil
}
