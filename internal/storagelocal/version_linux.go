//go:build linux

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
		modifiedSec: stat.Mtim.Sec,
		modifiedNS:  stat.Mtim.Nsec,
		changedSec:  stat.Ctim.Sec,
		changedNS:   stat.Ctim.Nsec,
	}, true
}
