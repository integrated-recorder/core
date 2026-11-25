//go:build !linux && !darwin

package replication

import "errors"

func replicationRenameNoReplace(string, string) error {
	return errors.New("atomic no-replace directory rename is unsupported")
}
