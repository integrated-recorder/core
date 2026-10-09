package authn

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	_ "time/tzdata"
)

const (
	userPreferencesDirectory = "user-preferences"
	userPreferencesVersion   = 1
	maxUserPreferencesBytes  = 4096
)

var (
	ErrInvalidPreferences            = errors.New("invalid user preferences")
	ErrUnsupportedPreferencesVersion = errors.New("unsupported user preferences version")
)

// UserPreferences contains presentation preferences scoped to one authenticated
// user. These values are control-plane state and never enter archive metadata.
type UserPreferences struct {
	Locale   string `json:"locale"`
	Theme    string `json:"theme"`
	Timezone string `json:"timezone"`
}

type persistedUserPreferences struct {
	Version int `json:"version"`
	UserPreferences
}

// UserPreferenceKey associates the storage location with a stable authenticated
// identity. HTTP callers cannot choose this key; routes use the session principal.
type UserPreferenceKey struct {
	UserID string `json:"user_id"`
}

func DefaultUserPreferences() UserPreferences {
	return UserPreferences{Locale: "system", Theme: "system", Timezone: "system"}
}

// ValidateUserPreferences validates the closed set of current preference
// values. Timezone accepts "system" or a loadable IANA location.
func ValidateUserPreferences(preferences UserPreferences) error {
	switch preferences.Locale {
	case "system", "ko-KR", "en-US":
	default:
		return ErrInvalidPreferences
	}
	switch preferences.Theme {
	case "system", "light", "dark":
	default:
		return ErrInvalidPreferences
	}
	if preferences.Timezone != "system" {
		if strings.TrimSpace(preferences.Timezone) != preferences.Timezone || preferences.Timezone == "" || preferences.Timezone == "Local" {
			return ErrInvalidPreferences
		}
		if _, err := time.LoadLocation(preferences.Timezone); err != nil {
			return ErrInvalidPreferences
		}
	}
	return nil
}

// GetUserPreferences reads one bounded per-user record. Missing or malformed
// preference data falls back to defaults without affecting authentication.
// The malformed file is left untouched until an explicit valid update.
func (s *Service) GetUserPreferences(userID string) (UserPreferences, error) {
	path, err := userPreferencesPath(filepath.Dir(s.securityDir), UserPreferenceKey{UserID: userID})
	if err != nil {
		return UserPreferences{}, err
	}
	persisted, err := readUserPreferences(path)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, ErrCorruptStore) || errors.Is(err, ErrUnsupportedPreferencesVersion) {
		return DefaultUserPreferences(), nil
	}
	if err != nil {
		return UserPreferences{}, err
	}
	return persisted.UserPreferences, nil
}

// SetUserPreferences atomically replaces the authenticated user's preferences.
// Current and unknown future JSON fields are forward-compatible on read, while
// an unsupported version is never overwritten by this generation.
func (s *Service) SetUserPreferences(userID string, preferences UserPreferences) error {
	if err := ValidateUserPreferences(preferences); err != nil {
		return err
	}
	path, err := userPreferencesPath(filepath.Dir(s.securityDir), UserPreferenceKey{UserID: userID})
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := securePrivateDirectory(filepath.Dir(filepath.Dir(path))); err != nil {
		return fmt.Errorf("%w: prepare management preferences root", ErrStorage)
	}
	if err := securePrivateDirectory(filepath.Dir(path)); err != nil {
		return fmt.Errorf("%w: prepare user preferences", ErrStorage)
	}
	if current, readErr := readUserPreferences(path); readErr == nil {
		if current.Version > userPreferencesVersion {
			return ErrUnsupportedPreferencesVersion
		}
	} else if !errors.Is(readErr, os.ErrNotExist) && !errors.Is(readErr, ErrCorruptStore) {
		return readErr
	}
	persisted := persistedUserPreferences{Version: userPreferencesVersion, UserPreferences: preferences}
	data, err := json.Marshal(persisted)
	if err != nil || len(data) > maxUserPreferencesBytes {
		return fmt.Errorf("%w: encode user preferences", ErrStorage)
	}
	data = append(data, '\n')
	if err := atomicWrite(path, data, 0600); err != nil {
		return err
	}
	return nil
}

// userPreferencesPath gives each user a stable private file beneath the
// management namespace, separate from system settings and authentication.
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

func readUserPreferences(path string) (persistedUserPreferences, error) {
	for _, directory := range []string{filepath.Dir(filepath.Dir(path)), filepath.Dir(path)} {
		directoryInfo, directoryErr := os.Lstat(directory)
		if errors.Is(directoryErr, os.ErrNotExist) {
			return persistedUserPreferences{}, os.ErrNotExist
		}
		if directoryErr != nil {
			return persistedUserPreferences{}, fmt.Errorf("%w: inspect user preferences directory", ErrStorage)
		}
		if !directoryInfo.IsDir() || directoryInfo.Mode()&os.ModeSymlink != 0 {
			return persistedUserPreferences{}, ErrCorruptStore
		}
	}
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return persistedUserPreferences{}, os.ErrNotExist
		}
		return persistedUserPreferences{}, fmt.Errorf("%w: inspect user preferences", ErrStorage)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maxUserPreferencesBytes {
		return persistedUserPreferences{}, ErrCorruptStore
	}
	file, err := os.Open(path)
	if err != nil {
		return persistedUserPreferences{}, fmt.Errorf("%w: open user preferences", ErrStorage)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return persistedUserPreferences{}, ErrCorruptStore
	}
	data, err := io.ReadAll(io.LimitReader(file, maxUserPreferencesBytes+1))
	if err != nil {
		return persistedUserPreferences{}, fmt.Errorf("%w: read user preferences", ErrStorage)
	}
	if len(data) == 0 || len(data) > maxUserPreferencesBytes {
		return persistedUserPreferences{}, ErrCorruptStore
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var persisted persistedUserPreferences
	if err := decoder.Decode(&persisted); err != nil {
		return persistedUserPreferences{}, ErrCorruptStore
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return persistedUserPreferences{}, ErrCorruptStore
	}
	if persisted.Version > userPreferencesVersion {
		return persistedUserPreferences{}, ErrUnsupportedPreferencesVersion
	}
	if persisted.Version != userPreferencesVersion || ValidateUserPreferences(persisted.UserPreferences) != nil {
		return persistedUserPreferences{}, ErrCorruptStore
	}
	return persisted, nil
}
