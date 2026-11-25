//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package discovery

import "errors"

func lockDirectory(string) (func(), error) {
	return nil, errors.New("discovery directory locking is unavailable on this platform")
}
