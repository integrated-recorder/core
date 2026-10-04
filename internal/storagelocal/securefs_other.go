//go:build !linux && !darwin

package storagelocal

import (
	"os"

	"github.com/dltkddnr04/integrated-recorder/internal/storageproto"
)

func openFilesystemRoot() (int, error)                      { return -1, storageproto.ErrUnsupported }
func openDirAt(int, string) (int, error)                    { return -1, storageproto.ErrUnsupported }
func openFileAt(int, string, int, os.FileMode) (int, error) { return -1, storageproto.ErrUnsupported }
func mkdirAt(int, string, os.FileMode) error                { return storageproto.ErrUnsupported }
func renameAt(int, string, int, string) error               { return storageproto.ErrUnsupported }
func unlinkAt(int, string) error                            { return storageproto.ErrUnsupported }
func fsyncDirectoryFD(int) error                            { return storageproto.ErrUnsupported }
func closeFD(int) error                                     { return nil }
func duplicateFD(int) (int, error)                          { return -1, storageproto.ErrUnsupported }
func fileLinkCount(os.FileInfo) uint64                      { return 1 }
func platformIsMissing(error) bool                          { return false }
func platformIsAlreadyExists(error) bool                    { return false }
func platformIsNotDirectory(error) bool                     { return false }
func platformIsSymlink(error) bool                          { return false }
func platformIsUnsafeEntry(error) bool                      { return false }
