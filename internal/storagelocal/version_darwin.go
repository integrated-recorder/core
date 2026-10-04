//go:build darwin

package storagelocal

import (
	"os"
	"syscall"
)

type objectVersion struct {
	device, inode           uint64
	size                    int64
	modifiedSec, modifiedNS int64
	changedSec, changedNS   int64
}

func versionOf(info os.FileInfo) (objectVersion, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Ino == 0 {
		return objectVersion{}, false
	}
	return objectVersion{
		device:      uint64(stat.Dev),
		inode:       stat.Ino,
		size:        info.Size(),
		modifiedSec: stat.Mtimespec.Sec,
		modifiedNS:  stat.Mtimespec.Nsec,
		changedSec:  stat.Ctimespec.Sec,
		changedNS:   stat.Ctimespec.Nsec,
	}, true
}
