//go:build darwin

package replication

import (
	"os"

	"golang.org/x/sys/unix"
)

func replicationRenameNoReplace(oldPath, newPath string) error {
	err := unix.RenamexNp(oldPath, newPath, unix.RENAME_EXCL)
	if err == unix.EEXIST {
		return os.ErrExist
	}
	return err
}
