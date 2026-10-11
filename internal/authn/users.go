package authn

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

var loginRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func (s *Service) loadOrMigrateUsers(allowToken bool) error {
	path := filepath.Join(s.securityDir, usersFilename)
	if _, err := readUserStore(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := s.migrateLegacyAdmin(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := readUserStore(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if allowToken {
		return ensureBootstrapToken(s.securityDir)
	}
	return nil
}

// migrateLegacyAdmin copies the existing bcrypt hash into owner identity.
// It never reads or reconstructs plaintext credentials. Atomic create makes
// concurrent Runtime Host and Control opens converge on one stable owner.
func (s *Service) migrateLegacyAdmin() error {
	legacyPath := filepath.Join(s.securityDir, adminFilename)
	hash, err := readAdminHashReadOnly(legacyPath)
	if err != nil {
		return err
	}
	owner, err := newUserRecord("owner", RoleOwner, string(hash), s.now())
	if err != nil {
		return err
	}
	data, err := encodeUserStore([]userRecord{owner})
	if err != nil {
		return err
	}
	if err := atomicCreate(filepath.Join(s.securityDir, usersFilename), data, 0600); err != nil {
		if errors.Is(err, os.ErrExist) {
			if _, readErr := readUserStore(filepath.Join(s.securityDir, usersFilename)); readErr == nil {
				return nil
			}
		}
		return err
	}
	return nil
}

func (s *Service) invalidateUnboundSessions() error {
	entries, err := os.ReadDir(s.sessionsDir)
	if err != nil {
		return fmt.Errorf("%w: inspect legacy sessions", ErrStorage)
	}
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		if len(name) != 2*32 {
			continue
		}
		if _, err := hex.DecodeString(name); err != nil {
			continue
		}
		path := filepath.Join(s.sessionsDir, name)
		record, err := readSessionRecordAt(path)
		if err != nil {
			return fmt.Errorf("%w: inspect legacy session", ErrStorage)
		}
		if record.userID == "" {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("%w: invalidate unbound session", ErrStorage)
			}
			removed = true
		}
	}
	if removed {
		if err := syncDirectory(s.sessionsDir); err != nil {
			return fmt.Errorf("%w: sync invalidated sessions", ErrStorage)
		}
	}
	return nil
}

func (s *Service) GetUser(id string) (User, error) {
	if !validUserID(id) {
		return User{}, ErrUserNotFound
	}
	var user User
	err := s.withAuthLock(func() error {
		record, err := findUserRecordByID(filepath.Join(s.securityDir, usersFilename), id)
		if err != nil {
			return err
		}
		user = record.User
		return nil
	})
	return user, err
}

func (s *Service) FindByLogin(login string) (User, error) {
	if !loginRE.MatchString(login) {
		return User{}, ErrUserNotFound
	}
	var user User
	err := s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return ErrNotBootstrapped
			}
			return err
		}
		for _, record := range store.Users {
			if strings.EqualFold(record.Login, login) {
				user = record.User
				return nil
			}
		}
		return ErrUserNotFound
	})
	return user, err
}

// ListUsers returns public identity metadata without password hashes.
func (s *Service) ListUsers() ([]User, error) {
	var result []User
	err := s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			return err
		}
		result = make([]User, 0, len(store.Users))
		for _, record := range store.Users {
			result = append(result, record.User)
		}
		sort.Slice(result, func(i, j int) bool { return strings.ToLower(result[i].Login) < strings.ToLower(result[j].Login) })
		return nil
	})
	return result, err
}

// CreateUser creates one account with the requested existing role.
func (s *Service) CreateUser(login, password, role string) (User, error) {
	if !loginRE.MatchString(login) || !validPasswordLength(password) || (role != RoleOwner && role != RoleMember) {
		return User{}, ErrInvalidCredentials
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost)
	if err != nil {
		return User{}, fmt.Errorf("%w: hash user password", ErrStorage)
	}
	var created userRecord
	err = s.withAuthLock(func() error {
		path := filepath.Join(s.securityDir, usersFilename)
		store, err := readUserStore(path)
		if err != nil {
			return err
		}
		for _, existing := range store.Users {
			if strings.EqualFold(existing.Login, login) {
				return ErrDuplicateLogin
			}
		}
		created, err = newUserRecord(login, role, string(hash), s.now())
		if err != nil {
			return err
		}
		store.Users = append(store.Users, created)
		return s.persistUserStore(store.Users)
	})
	return created.User, err
}

func (s *Service) VerifyCredentials(login, password string) (User, error) {
	if !validPasswordLength(password) || !loginRE.MatchString(login) {
		return User{}, ErrInvalidCredentials
	}
	var user User
	err := s.withAuthLock(func() error {
		record, err := s.verifyCredentialsLocked(login, password)
		if err != nil {
			return err
		}
		user = record.User
		return nil
	})
	return user, err
}

func (s *Service) verifyCredentialsLocked(login, password string) (userRecord, error) {
	if !validPasswordLength(password) || !loginRE.MatchString(login) {
		return userRecord{}, ErrInvalidCredentials
	}
	store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
	if errors.Is(err, os.ErrNotExist) {
		if _, legacyErr := readAdminHashReadOnly(filepath.Join(s.securityDir, adminFilename)); legacyErr == nil {
			return userRecord{}, ErrInvalidCredentials
		} else if !errors.Is(legacyErr, os.ErrNotExist) {
			return userRecord{}, ErrInvalidCredentials
		}
		return userRecord{}, ErrNotBootstrapped
	}
	if err != nil {
		return userRecord{}, ErrInvalidCredentials
	}
	for _, record := range store.Users {
		if strings.EqualFold(record.Login, login) && record.Status == UserStatusActive {
			if bcrypt.CompareHashAndPassword([]byte(record.PasswordHash), []byte(password)) == nil {
				return record, nil
			}
			return userRecord{}, ErrInvalidCredentials
		}
	}
	return userRecord{}, ErrInvalidCredentials
}

// ChangePassword verifies the current password and atomically advances the
// user's credential epoch with the new bcrypt hash.
func (s *Service) ChangePassword(userID, currentPassword, newPassword string) error {
	if !validPasswordLength(newPassword) {
		return ErrInvalidPassword
	}
	return s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			return err
		}
		index := userIndexByID(store.Users, userID)
		if index < 0 || store.Users[index].Status != UserStatusActive || !validPasswordLength(currentPassword) || bcrypt.CompareHashAndPassword([]byte(store.Users[index].PasswordHash), []byte(currentPassword)) != nil {
			return ErrCurrentPassword
		}
		return s.updatePasswordLocked(store, index, newPassword)
	})
}

// RecoverPassword is only called by the local runtime-host administrative
// command. HTTP routes never expose unauthenticated password recovery.
func (s *Service) RecoverPassword(login, newPassword string) (User, error) {
	if !loginRE.MatchString(login) || !validPasswordLength(newPassword) {
		return User{}, ErrInvalidPassword
	}
	var user User
	err := s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			return err
		}
		index := userIndexByLogin(store.Users, login)
		if index < 0 {
			return ErrUserNotFound
		}
		if err := s.updatePasswordLocked(store, index, newPassword); err != nil {
			return err
		}
		user = store.Users[index].User
		return nil
	})
	return user, err
}

func (s *Service) updatePasswordLocked(store persistedUserStore, index int, password string) error {
	if store.Users[index].CredentialEpoch == ^uint64(0) {
		return ErrStorage
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost)
	if err != nil {
		return fmt.Errorf("%w: update password hash", ErrStorage)
	}
	store.Users[index].PasswordHash = string(hash)
	store.Users[index].CredentialEpoch++
	if err := s.persistUserStore(store.Users); err != nil {
		return err
	}
	// Epoch mismatch is the security boundary. Removing persisted records is a
	// best-effort cleanup after the atomic user-store commit.
	_ = s.revokeUserSessionsLocked(store.Users[index].ID)
	return nil
}

// SetUserEnabled toggles an account and increments its epoch on each actual
// state transition, invalidating sessions before returning.
func (s *Service) SetUserEnabled(userID string, enabled bool) (User, error) {
	var result User
	err := s.withAuthLock(func() error {
		path := filepath.Join(s.securityDir, usersFilename)
		store, err := readUserStore(path)
		if err != nil {
			return err
		}
		index := userIndexByID(store.Users, userID)
		if index < 0 {
			return ErrUserNotFound
		}
		want := UserStatusDisabled
		if enabled {
			want = UserStatusActive
		}
		current := &store.Users[index]
		if current.Status == want {
			result = current.User
			return nil
		}
		if !enabled && current.Role == RoleOwner {
			activeOwners := 0
			for _, candidate := range store.Users {
				if candidate.Role == RoleOwner && candidate.Status == UserStatusActive {
					activeOwners++
				}
			}
			if activeOwners <= 1 {
				return ErrLastOwner
			}
		}
		if current.CredentialEpoch == ^uint64(0) {
			return ErrStorage
		}
		current.Status = want
		current.CredentialEpoch++
		if err := s.persistUserStore(store.Users); err != nil {
			return err
		}
		_ = s.revokeUserSessionsLocked(current.ID)
		result = current.User
		return nil
	})
	return result, err
}

// ListSessions returns active sessions for one user with no secret material.
func (s *Service) ListSessions(userID, currentToken string) ([]SessionInfo, error) {
	var result []SessionInfo
	err := s.withAuthLock(func() error {
		current, user, err := s.currentSessionLocked(userID, currentToken)
		if err != nil {
			return err
		}
		entries, err := os.ReadDir(s.sessionsDir)
		if err != nil {
			return ErrStorage
		}
		now := s.now()
		result = make([]SessionInfo, 0, len(entries))
		for _, entry := range entries {
			name := entry.Name()
			if !validSessionFileName(name) {
				continue
			}
			record, readErr := readSessionRecordAt(filepath.Join(s.sessionsDir, name))
			if readErr != nil || record.userID != userID || record.credentialEpoch != user.CredentialEpoch || !now.Before(record.expiresAt) {
				continue
			}
			id := record.id
			if id == "" {
				id = name
			}
			result = append(result, SessionInfo{ID: id, ExpiresAt: record.expiresAt, Current: id == current.id})
		}
		sort.Slice(result, func(i, j int) bool { return result[i].ExpiresAt.After(result[j].ExpiresAt) })
		return nil
	})
	return result, err
}

// RevokeSession revokes one session belonging to userID. It returns whether
// the selected session was the caller's current session.
func (s *Service) RevokeSession(userID, sessionID, currentToken string) (bool, error) {
	wasCurrent := false
	err := s.withAuthLock(func() error {
		if !validSessionID(sessionID) || !validUserID(userID) {
			return ErrUserNotFound
		}
		current, _, err := s.currentSessionLocked(userID, currentToken)
		if err != nil {
			return err
		}
		if err := validatePrivateDirectory(s.sessionsDir); err != nil {
			return ErrStorage
		}
		entries, err := os.ReadDir(s.sessionsDir)
		if err != nil {
			return ErrStorage
		}
		for _, entry := range entries {
			name := entry.Name()
			if !validSessionFileName(name) {
				continue
			}
			path := filepath.Join(s.sessionsDir, name)
			record, readErr := readSessionRecordAt(path)
			if readErr != nil || record.userID != userID {
				continue
			}
			id := record.id
			if id == "" {
				id = name
			}
			if id != sessionID {
				continue
			}
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return ErrStorage
			}
			if err := syncDirectory(s.sessionsDir); err != nil {
				return ErrStorage
			}
			wasCurrent = current.id == id
			return nil
		}
		return ErrUserNotFound
	})
	return wasCurrent, err
}

// RevokeOtherSessions revokes every active session for userID except the
// token used by this request.
func (s *Service) RevokeOtherSessions(userID, currentToken string) (int, error) {
	removed := 0
	err := s.withAuthLock(func() error {
		current, _, err := s.currentSessionLocked(userID, currentToken)
		if err != nil {
			return err
		}
		entries, err := os.ReadDir(s.sessionsDir)
		if err != nil {
			return ErrStorage
		}
		for _, entry := range entries {
			name := entry.Name()
			if !validSessionFileName(name) {
				continue
			}
			path := filepath.Join(s.sessionsDir, name)
			record, readErr := readSessionRecordAt(path)
			if readErr != nil || record.userID != userID {
				continue
			}
			id := record.id
			if id == "" {
				id = name
			}
			if id == current.id {
				continue
			}
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return ErrStorage
			}
			removed++
		}
		if removed > 0 {
			return syncDirectory(s.sessionsDir)
		}
		return nil
	})
	return removed, err
}

func (s *Service) currentSessionLocked(userID, token string) (sessionRecord, userRecord, error) {
	if !validUserID(userID) || token == "" {
		return sessionRecord{}, userRecord{}, ErrUnauthenticated
	}
	record, err := s.readSession(token)
	if err != nil || record.userID != userID || !s.now().Before(record.expiresAt) {
		return sessionRecord{}, userRecord{}, ErrUnauthenticated
	}
	user, err := findUserRecordByID(filepath.Join(s.securityDir, usersFilename), userID)
	if err != nil || user.Status != UserStatusActive || record.credentialEpoch != user.CredentialEpoch {
		return sessionRecord{}, userRecord{}, ErrUnauthenticated
	}
	return record, user, nil
}

func (s *Service) revokeUserSessionsLocked(userID string) error {
	entries, err := os.ReadDir(s.sessionsDir)
	if err != nil {
		return err
	}
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		if !validSessionFileName(name) {
			continue
		}
		path := filepath.Join(s.sessionsDir, name)
		record, err := readSessionRecordAt(path)
		if err != nil || record.userID != userID {
			continue
		}
		remove := s.removeSessionFile
		if remove == nil {
			remove = os.Remove
		}
		if err := remove(path); err == nil || errors.Is(err, os.ErrNotExist) {
			removed = true
		}
	}
	if removed {
		return syncDirectory(s.sessionsDir)
	}
	return nil
}

func userIndexByID(users []userRecord, id string) int {
	for i := range users {
		if users[i].ID == id {
			return i
		}
	}
	return -1
}

func userIndexByLogin(users []userRecord, login string) int {
	for i := range users {
		if strings.EqualFold(users[i].Login, login) {
			return i
		}
	}
	return -1
}

func findUserRecordByID(path, id string) (userRecord, error) {
	store, err := readUserStore(path)
	if errors.Is(err, os.ErrNotExist) {
		return userRecord{}, ErrNotBootstrapped
	}
	if err != nil {
		return userRecord{}, err
	}
	index := userIndexByID(store.Users, id)
	if index < 0 {
		return userRecord{}, ErrUserNotFound
	}
	return store.Users[index], nil
}

func (s *Service) persistUserStore(users []userRecord) error {
	data, err := encodeUserStore(users)
	if err != nil {
		return err
	}
	write := s.persistUsers
	if write == nil {
		write = func(path string, data []byte) error { return atomicWrite(path, data, 0600) }
	}
	if err := write(filepath.Join(s.securityDir, usersFilename), data); err != nil {
		if errors.Is(err, ErrStorage) {
			return err
		}
		return fmt.Errorf("%w: persist user store", ErrStorage)
	}
	return nil
}

func newUserRecord(login, role, passwordHash string, createdAt time.Time) (userRecord, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return userRecord{}, fmt.Errorf("%w: generate user identity", ErrStorage)
	}
	return userRecord{User: User{
		ID:        "usr-" + hex.EncodeToString(raw[:]),
		Login:     login,
		Role:      role,
		Status:    UserStatusActive,
		CreatedAt: createdAt.UTC(),
	}, PasswordHash: passwordHash, CredentialEpoch: 1}, nil
}

func encodeUserStore(users []userRecord) ([]byte, error) {
	data, err := json.Marshal(persistedUserStore{Version: userStoreVersion, Users: users})
	if err != nil || len(data) > maxUserStoreBytes {
		return nil, fmt.Errorf("%w: encode user store", ErrStorage)
	}
	return append(data, '\n'), nil
}

func readUserStore(path string) (persistedUserStore, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return persistedUserStore{}, os.ErrNotExist
		}
		return persistedUserStore{}, fmt.Errorf("%w: inspect user store", ErrStorage)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maxUserStoreBytes {
		return persistedUserStore{}, ErrCorruptStore
	}
	if err := os.Chmod(path, 0600); err != nil {
		return persistedUserStore{}, fmt.Errorf("%w: secure user store", ErrStorage)
	}
	f, err := os.Open(path)
	if err != nil {
		return persistedUserStore{}, fmt.Errorf("%w: read user store", ErrStorage)
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return persistedUserStore{}, ErrCorruptStore
	}
	data, err := io.ReadAll(io.LimitReader(f, maxUserStoreBytes+1))
	if err != nil || len(data) == 0 || len(data) > maxUserStoreBytes {
		return persistedUserStore{}, ErrCorruptStore
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var store persistedUserStore
	if decoder.Decode(&store) != nil || store.Version != userStoreVersion || len(store.Users) == 0 {
		return persistedUserStore{}, ErrCorruptStore
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) {
		return persistedUserStore{}, ErrCorruptStore
	}
	ids := make(map[string]struct{}, len(store.Users))
	logins := make(map[string]struct{}, len(store.Users))
	for _, record := range store.Users {
		if !validUserID(record.ID) || !loginRE.MatchString(record.Login) || (record.Role != RoleOwner && record.Role != RoleMember) || (record.Status != UserStatusActive && record.Status != UserStatusDisabled) || record.CreatedAt.IsZero() {
			return persistedUserStore{}, ErrCorruptStore
		}
		if _, err := bcrypt.Cost([]byte(record.PasswordHash)); err != nil {
			return persistedUserStore{}, ErrCorruptStore
		}
		idKey, loginKey := record.ID, strings.ToLower(record.Login)
		if _, ok := ids[idKey]; ok {
			return persistedUserStore{}, ErrCorruptStore
		}
		if _, ok := logins[loginKey]; ok {
			return persistedUserStore{}, ErrCorruptStore
		}
		ids[idKey], logins[loginKey] = struct{}{}, struct{}{}
	}
	return store, nil
}

func validUserID(id string) bool {
	if !strings.HasPrefix(id, "usr-") || len(id) != 36 {
		return false
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(id, "usr-"))
	return err == nil && len(raw) == 16
}
