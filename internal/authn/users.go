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
	store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return User{}, ErrNotBootstrapped
		}
		return User{}, err
	}
	for _, record := range store.Users {
		if record.ID == id {
			return record.User, nil
		}
	}
	return User{}, ErrUserNotFound
}

func (s *Service) FindByLogin(login string) (User, error) {
	if !loginRE.MatchString(login) {
		return User{}, ErrUserNotFound
	}
	store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return User{}, ErrNotBootstrapped
		}
		return User{}, err
	}
	for _, record := range store.Users {
		if strings.EqualFold(record.Login, login) {
			return record.User, nil
		}
	}
	return User{}, ErrUserNotFound
}

// CreateUser is an internal control-plane seam. No public user-management
// endpoint exposes it in this stage. Initial role remains owner-only.
func (s *Service) CreateUser(login, password, role string) (User, error) {
	if !loginRE.MatchString(login) || !validPasswordLength(password) || role != RoleOwner {
		return User{}, ErrInvalidCredentials
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost)
	if err != nil {
		return User{}, fmt.Errorf("%w: hash user password", ErrStorage)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.securityDir, usersFilename)
	store, err := readUserStore(path)
	if err != nil {
		return User{}, err
	}
	for _, existing := range store.Users {
		if strings.EqualFold(existing.Login, login) {
			return User{}, ErrDuplicateLogin
		}
	}
	created, err := newUserRecord(login, role, string(hash), s.now())
	if err != nil {
		return User{}, err
	}
	store.Users = append(store.Users, created)
	data, err := encodeUserStore(store.Users)
	if err != nil {
		return User{}, err
	}
	if err := atomicWrite(path, data, 0600); err != nil {
		return User{}, err
	}
	return created.User, nil
}

func (s *Service) VerifyCredentials(login, password string) (User, error) {
	if !validPasswordLength(password) || !loginRE.MatchString(login) {
		return User{}, ErrInvalidCredentials
	}
	store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
	if errors.Is(err, os.ErrNotExist) {
		if _, legacyErr := readAdminHashReadOnly(filepath.Join(s.securityDir, adminFilename)); legacyErr == nil {
			return User{}, ErrInvalidCredentials
		} else if !errors.Is(legacyErr, os.ErrNotExist) {
			return User{}, ErrInvalidCredentials
		}
		return User{}, ErrNotBootstrapped
	}
	if err != nil {
		return User{}, ErrInvalidCredentials
	}
	for _, record := range store.Users {
		if strings.EqualFold(record.Login, login) && record.Status == UserStatusActive {
			if bcrypt.CompareHashAndPassword([]byte(record.PasswordHash), []byte(password)) == nil {
				return record.User, nil
			}
			return User{}, ErrInvalidCredentials
		}
	}
	return User{}, ErrInvalidCredentials
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
	}, PasswordHash: passwordHash}, nil
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
		if !validUserID(record.ID) || !loginRE.MatchString(record.Login) || record.Role != RoleOwner || (record.Status != UserStatusActive && record.Status != "disabled") || record.CreatedAt.IsZero() {
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
