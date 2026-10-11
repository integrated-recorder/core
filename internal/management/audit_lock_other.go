//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package management

import "fmt"

func lockAuditFile(string) (func(), error) {
	return nil, fmt.Errorf("audit store locking is unavailable")
}
