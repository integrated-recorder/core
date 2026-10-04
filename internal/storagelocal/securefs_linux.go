//go:build linux

package storagelocal

import (
	"errors"
	"os"
	"syscall"
)

func openFilesystemRoot() (int, error) {
	return syscall.Open("/", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
}

func openDirAt(parent int, name string) (int, error) {
	return syscall.Openat(parent, name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
}

func openFileAt(parent int, name string, kind int, mode os.FileMode) (int, error) {
	flags := syscall.O_CLOEXEC | syscall.O_NOFOLLOW
	if kind == openReadNoFollow {
		flags |= syscall.O_RDONLY | syscall.O_NONBLOCK
	} else {
		flags |= syscall.O_WRONLY | syscall.O_CREAT | syscall.O_EXCL
	}
	return syscall.Openat(parent, name, flags, uint32(mode.Perm()))
}

func mkdirAt(parent int, name string, mode os.FileMode) error {
	return syscall.Mkdirat(parent, name, uint32(mode.Perm()))
}

func renameAt(oldParent int, oldName string, newParent int, newName string) error {
	return syscall.Renameat(oldParent, oldName, newParent, newName)
}

func unlinkAt(parent int, name string) error { return syscall.Unlinkat(parent, name) }
func fsyncDirectoryFD(fd int) error          { return syscall.Fsync(fd) }
func closeFD(fd int) error                   { return syscall.Close(fd) }

func duplicateFD(fd int) (int, error) {
	copyFD, err := syscall.Dup(fd)
	if err == nil {
		syscall.CloseOnExec(copyFD)
	}
	return copyFD, err
}

func fileLinkCount(info os.FileInfo) uint64 {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return uint64(stat.Nlink)
}

func platformIsMissing(err error) bool       { return errors.Is(err, syscall.ENOENT) }
func platformIsAlreadyExists(err error) bool { return errors.Is(err, syscall.EEXIST) }
func platformIsNotDirectory(err error) bool  { return errors.Is(err, syscall.ENOTDIR) }
func platformIsSymlink(err error) bool       { return errors.Is(err, syscall.ELOOP) }
func platformIsUnsafeEntry(err error) bool {
	return errors.Is(err, syscall.ENXIO) || errors.Is(err, syscall.ENODEV)
}
