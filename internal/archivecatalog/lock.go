package archivecatalog

import (
	"context"
	"os"
	"path/filepath"
)

func (s *Store) lock(ctx context.Context, archiveRoot string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ensurePrivateDir(archiveRoot); err != nil {
		return nil, err
	}
	path := filepath.Join(archiveRoot, ".catalog.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, privateFileMode)
	if err != nil {
		return nil, ErrUnavailable
	}
	info, statErr := os.Lstat(path)
	opened, fileErr := file.Stat()
	if statErr != nil || fileErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != privateFileMode || !os.SameFile(info, opened) {
		_ = file.Close()
		return nil, ErrInvalid
	}
	if err := catalogFlock(file); err != nil {
		_ = file.Close()
		return nil, ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		catalogFunlock(file)
		_ = file.Close()
		return nil, err
	}
	return func() {
		catalogFunlock(file)
		_ = file.Close()
	}, nil
}
