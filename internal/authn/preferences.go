package authn

import (
	"path/filepath"
	"strings"
)

const userPreferencesDirectory = "user-preferences"

// UserPreferenceKey associates future per-user presentation preferences with
// the stable authenticated identity. It deliberately defines no preference
// fields or behavior.
type UserPreferenceKey struct {
	UserID string `json:"user_id"`
}

// userPreferencesPath gives per-user preferences a private management
// namespace, separate from system-wide settings and authentication records.
func userPreferencesPath(dataRoot string, key UserPreferenceKey) (string, error) {
	if strings.TrimSpace(dataRoot) == "" || !validUserID(key.UserID) {
		return "", ErrUserNotFound
	}
	root, err := filepath.Abs(dataRoot)
	if err != nil {
		return "", ErrStorage
	}
	return filepath.Join(root, "management", userPreferencesDirectory, key.UserID+".json"), nil
}
