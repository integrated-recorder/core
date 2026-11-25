//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package nodeidentity

import "errors"

func lockNodeIdentity(string) (func(), error) {
	return func() {}, errors.New("node identity cross-process locking is unavailable on this platform")
}
