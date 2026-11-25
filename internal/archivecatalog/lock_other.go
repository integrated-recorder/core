//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package archivecatalog

import (
	"errors"
	"os"
)

var errLockUnsupported = errors.New("archive catalog locking is unsupported on this platform")

func catalogFlock(*os.File) error { return errLockUnsupported }
func catalogFunlock(*os.File)     {}
