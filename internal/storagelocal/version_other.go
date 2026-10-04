//go:build !linux && !darwin

package storagelocal

import "os"

// Unsupported platforms conservatively hash every object access.
type objectVersion struct{}

func versionOf(os.FileInfo) (objectVersion, bool) { return objectVersion{}, false }
