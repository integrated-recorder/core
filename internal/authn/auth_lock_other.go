//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package authn

// Authentication persistence requires a kernel-managed inter-process lock.
// Unsupported platforms fail closed instead of silently using a process-only
// mutex for shared durable state.
func lockAuthStoreFile(string) (func(), error) { return nil, ErrStorage }
