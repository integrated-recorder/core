package storagelocal

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dltkddnr04/integrated-recorder/internal/storageproto"
)

const (
	openReadNoFollow = iota
	openWriteCreateExclusive
)

func openRootPath(root string, create bool) (int, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return -1, fmt.Errorf("local storage root is invalid")
	}
	parentPath, err := filepath.EvalSymlinks(filepath.Dir(root))
	if err != nil {
		return -1, err
	}
	if !filepath.IsAbs(parentPath) {
		return -1, fmt.Errorf("local storage root parent is invalid")
	}
	parentFD, err := openAbsoluteDirectory(parentPath)
	if err != nil {
		return -1, err
	}
	base := filepath.Base(root)
	rootFD, err := openDirAt(parentFD, base)
	if err != nil && create && isMissing(err) {
		if mkdirErr := mkdirAt(parentFD, base, 0700); mkdirErr != nil && !isAlreadyExists(mkdirErr) {
			_ = closeFD(parentFD)
			return -1, mkdirErr
		}
		if syncErr := fsyncDirectoryFD(parentFD); syncErr != nil {
			_ = closeFD(parentFD)
			return -1, syncErr
		}
		rootFD, err = openDirAt(parentFD, base)
	}
	closeErr := closeFD(parentFD)
	if err != nil {
		return -1, err
	}
	if closeErr != nil {
		_ = closeFD(rootFD)
		return -1, closeErr
	}
	return rootFD, nil
}

func openAbsoluteDirectory(path string) (int, error) {
	fd, err := openFilesystemRoot()
	if err != nil {
		return -1, err
	}
	trimmed := strings.TrimPrefix(filepath.Clean(path), string(filepath.Separator))
	if trimmed == "" {
		return fd, nil
	}
	for _, component := range strings.Split(trimmed, string(filepath.Separator)) {
		if component == "" || component == "." || component == ".." {
			_ = closeFD(fd)
			return -1, fmt.Errorf("local storage root parent is invalid")
		}
		next, openErr := openDirAt(fd, component)
		_ = closeFD(fd)
		if openErr != nil {
			return -1, openErr
		}
		fd = next
	}
	return fd, nil
}

func isMissing(err error) bool {
	return errors.Is(err, storageproto.ErrNotFound) || errors.Is(err, os.ErrNotExist) || os.IsNotExist(err) || platformIsMissing(err)
}

func isAlreadyExists(err error) bool {
	return errors.Is(err, os.ErrExist) || os.IsExist(err) || platformIsAlreadyExists(err)
}

func isNotDirectory(err error) bool { return platformIsNotDirectory(err) }
func isSymlink(err error) bool      { return platformIsSymlink(err) }
func isUnsafeEntry(err error) bool  { return platformIsUnsafeEntry(err) }

func makeTemporaryName(prefix string) (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(random[:]), nil
}
