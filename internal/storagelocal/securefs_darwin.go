//go:build darwin

package storagelocal

import (
	"errors"
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

// Darwin's syscall package has openat stubs internally but does not export
// them. These syscall numbers are stable Darwin ABI entries from sys/syscall.h.
const (
	darwinSYSOpenat   = 463
	darwinSYSRenameat = 465
	darwinSYSUnlinkat = 472
	darwinSYSMkdirat  = 475
)

func openFilesystemRoot() (int, error) {
	return syscall.Open("/", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
}

func openDirAt(parent int, name string) (int, error) {
	return darwinOpenat(darwinSYSOpenat, parent, name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
}

func openFileAt(parent int, name string, kind int, mode os.FileMode) (int, error) {
	flags := syscall.O_CLOEXEC | syscall.O_NOFOLLOW
	if kind == openReadNoFollow {
		flags |= syscall.O_RDONLY | syscall.O_NONBLOCK
	} else {
		flags |= syscall.O_WRONLY | syscall.O_CREAT | syscall.O_EXCL
	}
	return darwinOpenat(darwinSYSOpenat, parent, name, flags, uint32(mode.Perm()))
}

func mkdirAt(parent int, name string, mode os.FileMode) error {
	_, err := darwinAtCall(darwinSYSMkdirat, uintptr(parent), name, uintptr(mode.Perm()))
	return err
}

func renameAt(oldParent int, oldName string, newParent int, newName string) error {
	oldPath, err := syscall.BytePtrFromString(oldName)
	if err != nil {
		return err
	}
	newPath, err := syscall.BytePtrFromString(newName)
	if err != nil {
		return err
	}
	_, _, errno := syscall.Syscall6(darwinSYSRenameat, uintptr(oldParent), uintptr(unsafe.Pointer(oldPath)), uintptr(newParent), uintptr(unsafe.Pointer(newPath)), 0, 0)
	runtime.KeepAlive(oldPath)
	runtime.KeepAlive(newPath)
	if errno != 0 {
		return errno
	}
	return nil
}

func unlinkAt(parent int, name string) error {
	_, err := darwinAtCall(darwinSYSUnlinkat, uintptr(parent), name, 0)
	return err
}

func fsyncDirectoryFD(fd int) error { return syscall.Fsync(fd) }
func closeFD(fd int) error          { return syscall.Close(fd) }

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

func darwinOpenat(number uintptr, parent int, path string, flags int, mode uint32) (int, error) {
	pathPointer, err := syscall.BytePtrFromString(path)
	if err != nil {
		return -1, err
	}
	result, _, errno := syscall.Syscall6(number, uintptr(parent), uintptr(unsafe.Pointer(pathPointer)), uintptr(flags), uintptr(mode), 0, 0)
	runtime.KeepAlive(pathPointer)
	if errno != 0 {
		return -1, errno
	}
	return int(result), nil
}

func darwinAtCall(number uintptr, parent uintptr, path string, final uintptr) (uintptr, error) {
	pathPointer, err := syscall.BytePtrFromString(path)
	if err != nil {
		return 0, err
	}
	result, _, errno := syscall.Syscall6(number, parent, uintptr(unsafe.Pointer(pathPointer)), final, 0, 0, 0)
	runtime.KeepAlive(pathPointer)
	if errno != 0 {
		return 0, errno
	}
	return result, nil
}
