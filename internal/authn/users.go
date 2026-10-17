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
	if store, err := readUserStore(path); err == nil {
		if store.migratedFromV1 {
			return s.withAuthLock(func() error {
				current, readErr := readUserStore(path)
				if readErr != nil {
					return readErr
				}
				if current.migratedFromV1 {
					return s.persistAuthState(current)
				}
				return nil
			})
		}
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
	owner, err := newUserRecord("owner", []string{RoleOwner}, string(hash), s.now())
	if err != nil {
		return err
	}
	data, err := encodeUserStore(persistedUserStore{Version: userStoreVersion, Users: []userRecord{owner}})
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
	return s.CreateUserWithRoles(login, password, []string{role})
}

func (s *Service) CreateUserWithRoles(login, password string, roles []string) (User, error) {
	if !loginRE.MatchString(login) || !validPasswordLength(password) {
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
		if err := validateRoleAssignments(roles, store.CustomRoles); err != nil {
			return err
		}
		created, err = newUserRecord(login, roles, string(hash), s.now())
		if err != nil {
			return err
		}
		store.Users = append(store.Users, created)
		return s.persistAuthState(store)
	})
	return created.User, err
}

func (s *Service) CreateUserWithRolesAs(actorID, login, password string, roles []string) (User, error) {
	var created User
	err := s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			return err
		}
		actor, err := userByID(store.Users, actorID)
		if err != nil || actor.Status != UserStatusActive {
			return ErrUnauthenticated
		}
		if err := validateRoleAssignments(roles, store.CustomRoles); err != nil {
			return err
		}
		if hasRole(roles, RoleOwner) && !hasRole(actor.Roles, RoleOwner) {
			return ErrOwnerAssignment
		}
		for _, existing := range store.Users {
			if strings.EqualFold(existing.Login, login) {
				return ErrDuplicateLogin
			}
		}
		if !loginRE.MatchString(login) || !validPasswordLength(password) {
			return ErrInvalidCredentials
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost)
		if err != nil {
			return fmt.Errorf("%w: hash user password", ErrStorage)
		}
		record, err := newUserRecord(login, roles, string(hash), s.now())
		if err != nil {
			return err
		}
		store.Users = append(store.Users, record)
		if err := s.persistAuthState(store); err != nil {
			return err
		}
		created = record.User
		return nil
	})
	return created, err
}

// RenameLogin validates the current password, preserves the stable user ID,
// writes the new login atomically, and advances the credential epoch.
func (s *Service) RenameLogin(userID, currentPassword, login string) (User, error) {
	if !loginRE.MatchString(login) {
		return User{}, ErrInvalidLogin
	}
	var result User
	err := s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			return err
		}
		index := userIndexByID(store.Users, userID)
		if index < 0 || store.Users[index].Status != UserStatusActive || !validPasswordLength(currentPassword) || bcrypt.CompareHashAndPassword([]byte(store.Users[index].PasswordHash), []byte(currentPassword)) != nil {
			return ErrCurrentPassword
		}
		for i, existing := range store.Users {
			if i != index && strings.EqualFold(existing.Login, login) {
				return ErrDuplicateLogin
			}
		}
		if store.Users[index].Login == login {
			result = store.Users[index].User
			return nil
		}
		if store.Users[index].CredentialEpoch == ^uint64(0) {
			return ErrStorage
		}
		store.Users[index].Login = login
		store.Users[index].CredentialEpoch++
		if err := s.persistAuthState(store); err != nil {
			return err
		}
		_ = s.revokeUserSessionsLocked(userID)
		result = store.Users[index].User
		return nil
	})
	return result, err
}

func (s *Service) AuthorizationFor(userID string) (Authorization, error) {
	var result Authorization
	err := s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			return err
		}
		user, err := userByID(store.Users, userID)
		if err != nil || user.Status != UserStatusActive {
			return ErrUserNotFound
		}
		result = Authorization{UserID: user.ID, Login: user.Login, Roles: append([]string(nil), user.Roles...), Permissions: permissionsForRoles(user.Roles, store.CustomRoles)}
		return nil
	})
	return result, err
}

func (s *Service) ListRoles() ([]Role, []Permission, error) {
	var roles []Role
	err := s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			return err
		}
		roles = BuiltInRoles()
		roles = append(roles, store.CustomRoles...)
		sort.Slice(roles, func(i, j int) bool { return roles[i].ID < roles[j].ID })
		return nil
	})
	return roles, PermissionCatalog(), err
}

func (s *Service) CreateCustomRole(name string, permissions []Permission) (Role, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 64 {
		return Role{}, ErrInvalidRole
	}
	permissions, err := validateRolePermissions(permissions)
	if err != nil {
		return Role{}, err
	}
	var result Role
	err = s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			return err
		}
		for _, existing := range store.CustomRoles {
			if strings.EqualFold(existing.Name, name) {
				return ErrInvalidRole
			}
		}
		id, err := randomRoleID()
		if err != nil {
			return err
		}
		created := s.now().UTC()
		result = Role{ID: id, Name: name, Permissions: permissions, CreatedAt: &created}
		store.CustomRoles = append(store.CustomRoles, result)
		return s.persistAuthState(store)
	})
	return result, err
}

func (s *Service) UpdateCustomRole(id, name string, permissions []Permission) (Role, error) {
	name = strings.TrimSpace(name)
	if !validRoleID(id) || name == "" || len(name) > 64 {
		return Role{}, ErrInvalidRole
	}
	if _, builtin := builtinRole(id); builtin {
		return Role{}, ErrBuiltinRole
	}
	permissions, err := validateRolePermissions(permissions)
	if err != nil {
		return Role{}, err
	}
	var result Role
	err = s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			return err
		}
		for index := range store.CustomRoles {
			if strings.EqualFold(store.CustomRoles[index].Name, name) && store.CustomRoles[index].ID != id {
				return ErrInvalidRole
			}
			if store.CustomRoles[index].ID == id {
				createdAt := store.CustomRoles[index].CreatedAt
				result = Role{ID: id, Name: name, Permissions: permissions, CreatedAt: createdAt}
				store.CustomRoles[index] = result
				if err := s.persistAuthState(store); err != nil {
					return err
				}
				return nil
			}
		}
		return ErrRoleNotFound
	})
	return result, err
}

func (s *Service) DeleteCustomRole(id string) error {
	if _, builtin := builtinRole(id); builtin {
		return ErrBuiltinRole
	}
	return s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			return err
		}
		roleIndex := -1
		for i, role := range store.CustomRoles {
			if role.ID == id {
				roleIndex = i
				break
			}
		}
		if roleIndex < 0 {
			return ErrRoleNotFound
		}
		for _, user := range store.Users {
			if hasRole(user.Roles, id) {
				return ErrRoleInUse
			}
		}
		store.CustomRoles = append(store.CustomRoles[:roleIndex], store.CustomRoles[roleIndex+1:]...)
		return s.persistAuthState(store)
	})
}

// SetUserRoles uses the same cross-process auth-store lock as owner-count
// checks and persistence so two concurrent mutations cannot remove the final
// active Owner.
func (s *Service) SetUserRoles(actorID, targetID string, roles []string) (User, error) {
	var result User
	err := s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			return err
		}
		actor, err := userByID(store.Users, actorID)
		if err != nil || actor.Status != UserStatusActive {
			return ErrUnauthenticated
		}
		index := userIndexByID(store.Users, targetID)
		if index < 0 {
			return ErrUserNotFound
		}
		if err := validateRoleAssignments(roles, store.CustomRoles); err != nil {
			return err
		}
		current := &store.Users[index]
		rootChange := hasRole(current.Roles, RoleOwner) || hasRole(roles, RoleOwner)
		if rootChange && !hasRole(actor.Roles, RoleOwner) {
			return ErrOwnerAssignment
		}
		if sameStrings(current.Roles, roles) {
			result = current.User
			return nil
		}
		if hasRole(current.Roles, RoleOwner) && !hasRole(roles, RoleOwner) && current.Status == UserStatusActive && activeOwnerCount(store.Users) <= 1 {
			return ErrLastOwner
		}
		if current.CredentialEpoch == ^uint64(0) {
			return ErrStorage
		}
		current.Roles = append([]string(nil), roles...)
		current.Role = primaryRole(roles)
		current.CredentialEpoch++
		if err := s.persistAuthState(store); err != nil {
			return err
		}
		_ = s.revokeUserSessionsLocked(targetID)
		result = current.User
		return nil
	})
	return result, err
}

func (s *Service) SetUserEnabledAs(actorID, targetID string, enabled bool) (User, error) {
	var result User
	err := s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			return err
		}
		actor, err := userByID(store.Users, actorID)
		if err != nil || actor.Status != UserStatusActive {
			return ErrUnauthenticated
		}
		index := userIndexByID(store.Users, targetID)
		if index < 0 {
			return ErrUserNotFound
		}
		target := &store.Users[index]
		if hasRole(target.Roles, RoleOwner) && !hasRole(actor.Roles, RoleOwner) {
			return ErrOwnerAssignment
		}
		if !enabled && target.Status == UserStatusActive && hasRole(target.Roles, RoleOwner) && activeOwnerCount(store.Users) <= 1 {
			return ErrLastOwner
		}
		want := UserStatusDisabled
		if enabled {
			want = UserStatusActive
		}
		if target.Status == want {
			result = target.User
			return nil
		}
		if target.CredentialEpoch == ^uint64(0) {
			return ErrStorage
		}
		target.Status = want
		target.CredentialEpoch++
		if err := s.persistAuthState(store); err != nil {
			return err
		}
		_ = s.revokeUserSessionsLocked(targetID)
		result = target.User
		return nil
	})
	return result, err
}

func (s *Service) DeleteUser(actorID, targetID string) (User, error) {
	var deleted User
	err := s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			return err
		}
		actor, err := userByID(store.Users, actorID)
		if err != nil || actor.Status != UserStatusActive {
			return ErrUnauthenticated
		}
		index := userIndexByID(store.Users, targetID)
		if index < 0 {
			return ErrUserNotFound
		}
		target := store.Users[index]
		if hasRole(target.Roles, RoleOwner) && !hasRole(actor.Roles, RoleOwner) {
			return ErrOwnerAssignment
		}
		if target.Status == UserStatusActive && hasRole(target.Roles, RoleOwner) && activeOwnerCount(store.Users) <= 1 {
			return ErrLastOwner
		}
		deleted = target.User
		store.Users = append(store.Users[:index], store.Users[index+1:]...)
		if len(store.Users) == 0 {
			return ErrLastOwner
		}
		if err := s.persistAuthState(store); err != nil {
			return err
		}
		_ = s.revokeUserSessionsLocked(targetID)
		return nil
	})
	return deleted, err
}

func userByID(users []userRecord, id string) (userRecord, error) {
	index := userIndexByID(users, id)
	if index < 0 {
		return userRecord{}, ErrUserNotFound
	}
	return users[index], nil
}

func activeOwnerCount(users []userRecord) int {
	count := 0
	for _, user := range users {
		if user.Status == UserStatusActive && hasRole(user.Roles, RoleOwner) {
			count++
		}
	}
	return count
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
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
	if err := s.persistAuthState(store); err != nil {
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
		if !enabled && hasRole(current.Roles, RoleOwner) {
			activeOwners := 0
			for _, candidate := range store.Users {
				if hasRole(candidate.Roles, RoleOwner) && candidate.Status == UserStatusActive {
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
		if err := s.persistAuthState(store); err != nil {
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

func (s *Service) persistAuthState(store persistedUserStore) error {
	store.Version = userStoreVersion
	store.migratedFromV1 = false
	data, err := encodeUserStore(store)
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

func newUserRecord(login string, roles []string, passwordHash string, createdAt time.Time) (userRecord, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return userRecord{}, fmt.Errorf("%w: generate user identity", ErrStorage)
	}
	return userRecord{User: User{
		ID:        "usr-" + hex.EncodeToString(raw[:]),
		Login:     login,
		Roles:     append([]string(nil), roles...),
		Role:      primaryRole(roles),
		Status:    UserStatusActive,
		CreatedAt: createdAt.UTC(),
	}, PasswordHash: passwordHash, CredentialEpoch: 1}, nil
}

func encodeUserStore(input any) ([]byte, error) {
	var store persistedUserStore
	switch value := input.(type) {
	case persistedUserStore:
		store = value
	case []userRecord: // legacy in-package test fixtures
		store = persistedUserStore{Version: userStoreVersion, Users: value}
	default:
		return nil, fmt.Errorf("%w: encode user store", ErrStorage)
	}
	store.Version = userStoreVersion
	store.migratedFromV1 = false
	data, err := json.Marshal(store)
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
	var version struct {
		Version int `json:"version"`
	}
	if json.Unmarshal(data, &version) != nil {
		return persistedUserStore{}, ErrCorruptStore
	}
	var store persistedUserStore
	if version.Version == 1 {
		var legacy legacyPersistedUserStore
		legacyDecoder := json.NewDecoder(strings.NewReader(string(data)))
		legacyDecoder.DisallowUnknownFields()
		if legacyDecoder.Decode(&legacy) != nil || legacy.Version != 1 || len(legacy.Users) == 0 {
			return persistedUserStore{}, ErrCorruptStore
		}
		store = persistedUserStore{Version: userStoreVersion, migratedFromV1: true}
		for _, old := range legacy.Users {
			role := RoleOperator
			if old.Role == RoleOwner {
				role = RoleOwner
			} else if old.Role != "member" {
				return persistedUserStore{}, ErrCorruptStore
			}
			store.Users = append(store.Users, userRecord{User: User{ID: old.ID, Login: old.Login, Roles: []string{role}, Role: role, Status: old.Status, CreatedAt: old.CreatedAt}, PasswordHash: old.PasswordHash, CredentialEpoch: old.CredentialEpoch})
		}
	} else if version.Version == userStoreVersion {
		decoder := json.NewDecoder(strings.NewReader(string(data)))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&store) != nil || store.Version != userStoreVersion || len(store.Users) == 0 {
			return persistedUserStore{}, ErrCorruptStore
		}
		var extra any
		if !errors.Is(decoder.Decode(&extra), io.EOF) {
			return persistedUserStore{}, ErrCorruptStore
		}
	} else {
		return persistedUserStore{}, ErrCorruptStore
	}
	for i := range store.Users {
		store.Users[i].Role = primaryRole(store.Users[i].Roles)
	}
	ids := make(map[string]struct{}, len(store.Users))
	logins := make(map[string]struct{}, len(store.Users))
	for _, record := range store.Users {
		if !validUserID(record.ID) || !loginRE.MatchString(record.Login) || !allRoleIDsKnown(record.Roles, store.CustomRoles) || (record.Status != UserStatusActive && record.Status != UserStatusDisabled) || record.CreatedAt.IsZero() {
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
	if activeOwnerCount(store.Users) == 0 {
		return persistedUserStore{}, ErrCorruptStore
	}
	customIDs := make(map[string]struct{}, len(store.CustomRoles))
	customNames := make(map[string]struct{}, len(store.CustomRoles))
	for _, role := range store.CustomRoles {
		if !validRoleID(role.ID) || role.BuiltIn || strings.TrimSpace(role.Name) == "" || len(role.Name) > 64 {
			return persistedUserStore{}, ErrCorruptStore
		}
		if _, builtin := builtinRole(role.ID); builtin {
			return persistedUserStore{}, ErrCorruptStore
		}
		if _, err := validateRolePermissions(role.Permissions); err != nil {
			return persistedUserStore{}, ErrCorruptStore
		}
		nameKey := strings.ToLower(strings.TrimSpace(role.Name))
		if _, duplicate := customIDs[role.ID]; duplicate {
			return persistedUserStore{}, ErrCorruptStore
		}
		if _, duplicate := customNames[nameKey]; duplicate {
			return persistedUserStore{}, ErrCorruptStore
		}
		customIDs[role.ID], customNames[nameKey] = struct{}{}, struct{}{}
	}
	return store, nil
}

func primaryRole(roles []string) string {
	if len(roles) == 0 {
		return ""
	}
	for _, role := range roles {
		if role == RoleOwner {
			return RoleOwner
		}
	}
	return roles[0]
}

func validUserID(id string) bool {
	if !strings.HasPrefix(id, "usr-") || len(id) != 36 {
		return false
	}
	raw, err := hex.DecodeString(strings.TrimPrefix(id, "usr-"))
	return err == nil && len(raw) == 16
}
