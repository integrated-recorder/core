// Package authn provides local user identity, sessions, and authorization.
// It intentionally contains no HTTP routes or server path policy.
package authn

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const (
	securityDirectory      = "security"
	sessionDirectory       = "sessions"
	adminFilename          = "admin.json"
	usersFilename          = "users.json"
	CSRFCookieName         = "ir_csrf"
	maxPasswordBytes       = 72 // bcrypt's input limit.
	minPasswordBytes       = 12
	maxActiveSessions      = 1024
	defaultSessionLifetime = 12 * time.Hour
	defaultBcryptCost      = bcrypt.DefaultCost
	maxSessionRecordBytes  = 1024
	maxUserStoreBytes      = 1 << 20
)

var (
	ErrBootstrapUnavailable = errors.New("authentication bootstrap is unavailable")
	ErrInvalidPassword      = errors.New("password must be between 12 and 72 bytes")
	ErrInvalidLogin         = errors.New("login must use 1-64 ASCII letters, digits, dot, underscore, or hyphen")
	ErrInvalidCredentials   = errors.New("invalid credentials")
	ErrUnauthenticated      = errors.New("unauthenticated")
	ErrSessionCapacity      = errors.New("session capacity reached")
	ErrNotBootstrapped      = errors.New("administrator is not configured")
	ErrUserNotFound         = errors.New("user not found")
	ErrDuplicateLogin       = errors.New("login identity already exists")
	ErrCurrentPassword      = errors.New("current credentials could not be verified")
	ErrLastOwner            = errors.New("the last active owner cannot be disabled")
	ErrSelfDisable          = errors.New("the current user cannot disable their own account")
	ErrStorage              = errors.New("authentication storage failure")
	ErrCorruptStore         = errors.New("authentication store is invalid")
)

// Session is returned to the caller exactly once after login. The session
// token is omitted from JSON so callers can place it in an HttpOnly cookie;
// the CSRF token remains available for request-header validation.
type Session struct {
	ID          string       `json:"-"`
	Token       string       `json:"-"`
	CSRFToken   string       `json:"csrf_token,omitempty"`
	ExpiresAt   time.Time    `json:"expires_at"`
	UserID      string       `json:"-"`
	Login       string       `json:"-"`
	Roles       []string     `json:"-"`
	Permissions []Permission `json:"-"`
	Role        string       `json:"-"` // deprecated display compatibility; never used for authorization
}

type adminRecord struct {
	PasswordHash string `json:"password_hash"`
}

// User is safe identity metadata. Password hashes never leave the private
// persisted user record.
type User struct {
	ID        string    `json:"user_id"`
	Login     string    `json:"login"`
	Roles     []string  `json:"roles"`
	Role      string    `json:"role,omitempty"` // compatibility projection, always derived from Roles
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type userRecord struct {
	User
	PasswordHash    string `json:"password_hash"`
	CredentialEpoch uint64 `json:"credential_epoch,omitempty"`
}

type persistedUserStore struct {
	Version        int          `json:"version"`
	Users          []userRecord `json:"users"`
	CustomRoles    []Role       `json:"custom_roles,omitempty"`
	migratedFromV1 bool         `json:"-"`
}

type legacyUserRecord struct {
	ID              string    `json:"user_id"`
	Login           string    `json:"login"`
	Role            string    `json:"role"`
	Status          string    `json:"status"`
	CreatedAt       time.Time `json:"created_at"`
	PasswordHash    string    `json:"password_hash"`
	CredentialEpoch uint64    `json:"credential_epoch,omitempty"`
}

type legacyPersistedUserStore struct {
	Version int                `json:"version"`
	Users   []legacyUserRecord `json:"users"`
}

const userStoreVersion = 2

type sessionRecord struct {
	id              string
	csrfHash        [sha256.Size]byte
	expiresAt       time.Time
	userID          string
	credentialEpoch uint64
}

type persistedSessionRecord struct {
	ID              string    `json:"id,omitempty"`
	CSRFHash        string    `json:"csrf_hash"`
	ExpiresAt       time.Time `json:"expires_at"`
	UserID          string    `json:"user_id,omitempty"`
	CredentialEpoch uint64    `json:"credential_epoch,omitempty"`
}

// Service manages local users and sessions persisted as private,
// per-session records. Per-session records allow overlapping control-plane
// generations to share session validity without read-modify-write races on a
// shared session-map file.
type Service struct {
	mu sync.Mutex

	securityDir          string
	sessionsDir          string
	allowBootstrap       bool
	removeSessionFile    func(string) error
	syncSessionDirectory func(string) error
	persistUsers         func(string, []byte) error

	now        func() time.Time
	bcryptCost int
}

// SessionInfo is a privacy-minimal projection of one active session. IDs are
// opaque revocation handles; raw tokens and CSRF values are never included.
type SessionInfo struct {
	ID        string    `json:"id"`
	ExpiresAt time.Time `json:"expires_at"`
	Current   bool      `json:"current"`
}

// Open loads authentication state and permits the one-time first Owner
// creation while the installation is uninitialized.
func Open(dataRoot string) (*Service, error) {
	return open(dataRoot, true)
}

// withAuthLock serializes security state transitions both within a process
// and across Runtime Host/Control processes that share the same data root.
func (s *Service) withAuthLock(run func() error) error {
	if s == nil || run == nil {
		return ErrStorage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := lockAuthStoreFile(s.securityDir)
	if err != nil {
		return err
	}
	defer release()
	return run()
}

// OpenWithoutBootstrap opens existing authentication state without creating a
// new bootstrap credential. Runtime Host uses this in recovery mode so a
// missing administrator can never silently turn a ready installation into a
// claimable one.
func OpenWithoutBootstrap(dataRoot string) (*Service, error) {
	return open(dataRoot, false)
}

func open(dataRoot string, allowBootstrap bool) (*Service, error) {
	if strings.TrimSpace(dataRoot) == "" {
		return nil, ErrStorage
	}
	root, err := filepath.Abs(dataRoot)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve data root", ErrStorage)
	}
	securityDir := filepath.Join(root, securityDirectory)
	if err := os.MkdirAll(securityDir, 0700); err != nil {
		return nil, fmt.Errorf("%w: create security directory", ErrStorage)
	}
	info, err := os.Lstat(securityDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrStorage
	}
	if err := os.Chmod(securityDir, 0700); err != nil {
		return nil, fmt.Errorf("%w: secure security directory", ErrStorage)
	}
	sessionsDir := filepath.Join(securityDir, sessionDirectory)
	if err := os.MkdirAll(sessionsDir, 0700); err != nil {
		return nil, fmt.Errorf("%w: create session directory", ErrStorage)
	}
	if err := securePrivateDirectory(sessionsDir); err != nil {
		return nil, fmt.Errorf("%w: secure session directory", ErrStorage)
	}

	s := &Service{
		securityDir:          securityDir,
		sessionsDir:          sessionsDir,
		now:                  time.Now,
		bcryptCost:           defaultBcryptCost,
		allowBootstrap:       allowBootstrap,
		removeSessionFile:    os.Remove,
		syncSessionDirectory: syncDirectory,
	}
	if err := s.loadOrMigrateUsers(); err != nil {
		if !allowBootstrap && errors.Is(err, ErrCorruptStore) {
			// Recovery mode can serve its bounded UI, but corrupt identity state
			// never becomes claimable or writable.
			return s, nil
		}
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(securityDir, usersFilename)); err == nil {
		// Older session records have no user binding. Invalidate them on the
		// identity model transition instead of guessing which user owns them.
		if err := s.withAuthLock(s.invalidateUnboundSessions); err != nil {
			return nil, err
		}
		if err := syncDirectory(securityDir); err != nil {
			return nil, fmt.Errorf("%w: sync security directory", ErrStorage)
		}
	}
	// RC8 wrote a one-time token before an Owner existed. It is no longer an
	// authentication input; remove that legacy artifact only after the current
	// user store has been validated or migrated successfully. A corrupt store
	// returns above and therefore never reopens bootstrap.
	legacyTokenPath := filepath.Join(securityDir, "bootstrap-token")
	if _, err := os.Lstat(legacyTokenPath); err == nil {
		if err := removeIfExists(legacyTokenPath); err != nil {
			return nil, fmt.Errorf("%w: remove legacy bootstrap token", ErrStorage)
		}
		if err := syncDirectory(securityDir); err != nil {
			return nil, fmt.Errorf("%w: sync security directory", ErrStorage)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: inspect legacy bootstrap token", ErrStorage)
	}
	return s, nil
}

// InspectAdministrator validates the persisted administrator record without
// creating a token, changing permissions, or otherwise mutating auth state.
func InspectAdministrator(dataRoot string) (bool, error) {
	if strings.TrimSpace(dataRoot) == "" {
		return false, ErrStorage
	}
	root, err := filepath.Abs(dataRoot)
	if err != nil {
		return false, ErrStorage
	}
	securityDir := filepath.Join(root, securityDirectory)
	info, err := os.Lstat(securityDir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, ErrStorage
	}
	users, err := readUserStore(filepath.Join(securityDir, usersFilename))
	if err == nil {
		return len(users.Users) > 0, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	_, err = readAdminHashReadOnly(filepath.Join(securityDir, adminFilename))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

// AdministratorConfigured re-reads the shared durable user store so Host
// and Control instances observe a bootstrap performed by the other process.
func (s *Service) AdministratorConfigured() (bool, error) {
	if s == nil {
		return false, ErrStorage
	}
	users, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
	if err == nil {
		return len(users.Users) > 0, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	// An older installation may have been created before this service opened.
	// Migrate its existing bcrypt hash without needing plaintext credentials.
	if err := s.migrateLegacyAdmin(); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	users, err = readUserStore(filepath.Join(s.securityDir, usersFilename))
	if err != nil {
		return false, err
	}
	return len(users.Users) > 0, nil
}

// NeedsBootstrap is retained for compatibility. Read failures fail closed by
// reporting that bootstrap is unavailable.
func (s *Service) NeedsBootstrap() bool {
	configured, err := s.AdministratorConfigured()
	return err != nil || !configured
}

// Bootstrap atomically creates the first Owner. The durable users file is the
// one-time marker; no separate claim credential exists.
func (s *Service) Bootstrap(login, password string) error {
	if !loginRE.MatchString(login) {
		return ErrInvalidLogin
	}
	if !validPasswordLength(password) {
		return ErrInvalidPassword
	}
	return s.withAuthLock(func() error {
		if !s.allowBootstrap {
			return ErrBootstrapUnavailable
		}
		if users, err := readUserStore(filepath.Join(s.securityDir, usersFilename)); err == nil && len(users.Users) > 0 {
			return ErrBootstrapUnavailable
		} else if !errors.Is(err, os.ErrNotExist) {
			return ErrBootstrapUnavailable
		}
		if _, err := readAdminHashReadOnly(filepath.Join(s.securityDir, adminFilename)); err == nil {
			return ErrBootstrapUnavailable
		} else if !errors.Is(err, os.ErrNotExist) {
			return ErrBootstrapUnavailable
		}

		hash, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost)
		if err != nil {
			return fmt.Errorf("%w: hash administrator password", ErrStorage)
		}
		owner, err := newUserRecord(login, []string{RoleOwner}, string(hash), s.now())
		if err != nil {
			return err
		}
		data, err := encodeUserStore(persistedUserStore{Version: userStoreVersion, Users: []userRecord{owner}})
		if err != nil {
			return err
		}
		if err := atomicCreate(filepath.Join(s.securityDir, usersFilename), data, 0600); err != nil {
			if errors.Is(err, os.ErrExist) {
				return ErrBootstrapUnavailable
			}
			return err
		}
		return nil
	})
}

// NeedsBootstrap reports whether the administrator has not yet been set up.

// Login is an internal legacy helper only. Browser and HTTP callers must use
// LoginAs with an explicit mutable login identity. For old local callers it
// resolves the active Owner by role rather than assuming login == "owner".
func (s *Service) Login(password string) (Session, error) {
	var login string
	err := s.withAuthLock(func() error {
		store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if err != nil {
			return err
		}
		for _, user := range store.Users {
			if user.Status == UserStatusActive && hasRole(user.Roles, RoleOwner) {
				login = user.Login
				break
			}
		}
		if login == "" {
			return ErrInvalidCredentials
		}
		return nil
	})
	if err != nil {
		return Session{}, ErrInvalidCredentials
	}
	return s.LoginAs(login, password)
}

// LoginAs verifies a login identity and creates a durable, user-bound session.
func (s *Service) LoginAs(login, password string) (Session, error) {
	if !validPasswordLength(password) {
		return Session{}, ErrInvalidCredentials
	}
	token, err := randomToken()
	if err != nil {
		return Session{}, fmt.Errorf("%w: generate session", ErrStorage)
	}
	csrf, err := randomToken()
	if err != nil {
		return Session{}, fmt.Errorf("%w: generate CSRF token", ErrStorage)
	}
	id, err := randomSessionID()
	if err != nil {
		return Session{}, fmt.Errorf("%w: generate session identity", ErrStorage)
	}
	now := s.now()
	session := Session{ID: id, Token: token, CSRFToken: csrf, ExpiresAt: now.Add(defaultSessionLifetime)}
	csrfHash := sha256.Sum256([]byte(csrf))
	err = s.withAuthLock(func() error {
		user, verifyErr := s.verifyCredentialsLocked(login, password)
		if verifyErr != nil {
			return verifyErr
		}
		store, storeErr := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if storeErr != nil {
			return storeErr
		}
		session.UserID, session.Login = user.ID, user.Login
		session.Roles = append([]string(nil), user.Roles...)
		session.Permissions = permissionsForRoles(user.Roles, store.CustomRoles)
		session.Role = primaryRole(user.Roles)
		active, countErr := s.countAndCleanSessions(now)
		if countErr != nil {
			return countErr
		}
		if active >= maxActiveSessions {
			return ErrSessionCapacity
		}
		record := sessionRecord{id: id, csrfHash: csrfHash, expiresAt: session.ExpiresAt, userID: user.ID, credentialEpoch: user.CredentialEpoch}
		if writeErr := s.writeSession(token, record); writeErr != nil {
			return writeErr
		}
		return nil
	})
	if errors.Is(err, ErrInvalidCredentials) {
		return Session{}, ErrInvalidCredentials
	}
	if err != nil {
		return Session{}, err
	}
	return session, nil
}

// Authenticate validates a session token. Returned Session intentionally
// omits the token and CSRF secret; those are only available from Login.
func (s *Service) Authenticate(token string) (Session, error) {
	if token == "" {
		return Session{}, ErrUnauthenticated
	}
	var session Session
	err := s.withAuthLock(func() error {
		record, readErr := s.readSession(token)
		if readErr != nil {
			return ErrUnauthenticated
		}
		if !s.now().Before(record.expiresAt) || !validUserID(record.userID) {
			_ = s.removeSession(token)
			return ErrUnauthenticated
		}
		user, userErr := findUserRecordByID(filepath.Join(s.securityDir, usersFilename), record.userID)
		if userErr != nil || user.Status != UserStatusActive || record.credentialEpoch != user.CredentialEpoch {
			_ = s.removeSession(token)
			return ErrUnauthenticated
		}
		store, storeErr := readUserStore(filepath.Join(s.securityDir, usersFilename))
		if storeErr != nil {
			return storeErr
		}
		session = Session{ID: record.id, ExpiresAt: record.expiresAt, UserID: user.ID, Login: user.Login,
			Roles: append([]string(nil), user.Roles...), Permissions: permissionsForRoles(user.Roles, store.CustomRoles), Role: primaryRole(user.Roles)}
		return nil
	})
	if err != nil {
		return Session{}, ErrUnauthenticated
	}
	return session, nil
}

// Logout durably revokes a session. Unknown and empty tokens are harmless.
func (s *Service) Logout(token string) error {
	if token == "" {
		return nil
	}
	return s.withAuthLock(func() error { return s.removeSession(token) })
}

// ValidCSRF compares the supplied value with the CSRF token bound to a live
// session token. Both values are hashed before constant-time comparison.
func (s *Service) ValidCSRF(sessionToken, supplied string) bool {
	if sessionToken == "" {
		return false
	}
	suppliedHash := sha256.Sum256([]byte(supplied))
	valid := false
	if err := s.withAuthLock(func() error {
		record, readErr := s.readSession(sessionToken)
		if readErr != nil {
			return nil
		}
		if !s.now().Before(record.expiresAt) {
			_ = s.removeSession(sessionToken)
			return nil
		}
		if !validUserID(record.userID) {
			return nil
		}
		user, userErr := findUserRecordByID(filepath.Join(s.securityDir, usersFilename), record.userID)
		valid = userErr == nil && user.Status == UserStatusActive && record.credentialEpoch == user.CredentialEpoch && subtle.ConstantTimeCompare(record.csrfHash[:], suppliedHash[:]) == 1
		return nil
	}); err != nil {
		return false
	}
	return valid
}

func (s *Service) readSession(token string) (sessionRecord, error) {
	if token == "" {
		return sessionRecord{}, os.ErrNotExist
	}
	if err := validatePrivateDirectory(s.sessionsDir); err != nil {
		return sessionRecord{}, err
	}
	record, err := readSessionRecordAt(s.sessionPath(token))
	if err == nil && record.id == "" {
		record.id = sessionFilenameID(token)
	}
	return record, err
}

func readSessionRecordAt(path string) (sessionRecord, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return sessionRecord{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maxSessionRecordBytes {
		return sessionRecord{}, ErrCorruptStore
	}
	f, err := os.Open(path)
	if err != nil {
		return sessionRecord{}, err
	}
	defer f.Close()
	openedInfo, err := f.Stat()
	if err != nil || !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
		return sessionRecord{}, ErrCorruptStore
	}
	data, err := io.ReadAll(io.LimitReader(f, maxSessionRecordBytes+1))
	if err != nil {
		return sessionRecord{}, err
	}
	if len(data) == 0 || len(data) > maxSessionRecordBytes {
		return sessionRecord{}, ErrCorruptStore
	}
	return decodeSessionRecord(data)
}

func (s *Service) writeSession(token string, record sessionRecord) error {
	if err := securePrivateDirectory(s.sessionsDir); err != nil {
		return fmt.Errorf("%w: secure session directory", ErrStorage)
	}
	persisted := persistedSessionRecord{ID: record.id, CSRFHash: hex.EncodeToString(record.csrfHash[:]), ExpiresAt: record.expiresAt.UTC(), UserID: record.userID, CredentialEpoch: record.credentialEpoch}
	data, err := json.Marshal(persisted)
	if err != nil {
		return fmt.Errorf("%w: encode session record", ErrStorage)
	}
	if len(data) > maxSessionRecordBytes {
		return ErrCorruptStore
	}
	if err := atomicWrite(s.sessionPath(token), append(data, '\n'), 0600); err != nil {
		return err
	}
	return nil
}

func (s *Service) removeSession(token string) error {
	if token == "" {
		return nil
	}
	if validatePrivateDirectory(s.sessionsDir) != nil {
		return fmt.Errorf("%w: validate session directory", ErrStorage)
	}
	remove := s.removeSessionFile
	if remove == nil {
		remove = os.Remove
	}
	if err := remove(s.sessionPath(token)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: revoke session", ErrStorage)
	}
	syncDir := s.syncSessionDirectory
	if syncDir == nil {
		syncDir = syncDirectory
	}
	if err := syncDir(s.sessionsDir); err != nil {
		return fmt.Errorf("%w: persist session revocation", ErrStorage)
	}
	return nil
}

func (s *Service) sessionPath(token string) string {
	hash := hashSessionToken(token)
	return filepath.Join(s.sessionsDir, hex.EncodeToString(hash[:]))
}

func (s *Service) countAndCleanSessions(now time.Time) (int, error) {
	if err := validatePrivateDirectory(s.sessionsDir); err != nil {
		return 0, fmt.Errorf("%w: inspect session directory", ErrStorage)
	}
	entries, err := os.ReadDir(s.sessionsDir)
	if err != nil {
		return 0, fmt.Errorf("%w: read session directory", ErrStorage)
	}
	store, err := readUserStore(filepath.Join(s.securityDir, usersFilename))
	if err != nil {
		return 0, fmt.Errorf("%w: inspect users for sessions", ErrStorage)
	}
	users := make(map[string]userRecord, len(store.Users))
	for _, user := range store.Users {
		users[user.ID] = user
	}
	active := 0
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		if len(name) != sha256.Size*2 {
			continue
		}
		if _, err := hex.DecodeString(name); err != nil {
			continue
		}
		path := filepath.Join(s.sessionsDir, name)
		record, err := readSessionRecordAt(path)
		if err != nil {
			// Corrupt or linked records are never valid sessions. Login fails
			// closed instead of silently counting attacker-controlled state.
			return 0, fmt.Errorf("%w: inspect session record", ErrStorage)
		}
		user, exists := users[record.userID]
		if !now.Before(record.expiresAt) || !validUserID(record.userID) || !exists || user.Status != UserStatusActive || user.CredentialEpoch != record.credentialEpoch {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return 0, fmt.Errorf("%w: remove expired session", ErrStorage)
			}
			removed = true
			continue
		}
		active++
	}
	if removed {
		if err := syncDirectory(s.sessionsDir); err != nil {
			return 0, fmt.Errorf("%w: sync expired session cleanup", ErrStorage)
		}
	}
	return active, nil
}

func decodeSessionRecord(data []byte) (sessionRecord, error) {
	if len(data) == 0 || len(data) > maxSessionRecordBytes {
		return sessionRecord{}, ErrCorruptStore
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var persisted persistedSessionRecord
	if err := decoder.Decode(&persisted); err != nil {
		return sessionRecord{}, ErrCorruptStore
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return sessionRecord{}, ErrCorruptStore
	}
	csrfBytes, err := hex.DecodeString(persisted.CSRFHash)
	if err != nil || len(csrfBytes) != sha256.Size || persisted.ExpiresAt.IsZero() {
		return sessionRecord{}, ErrCorruptStore
	}
	var csrfHash [sha256.Size]byte
	copy(csrfHash[:], csrfBytes)
	if persisted.UserID != "" && !validUserID(persisted.UserID) {
		return sessionRecord{}, ErrCorruptStore
	}
	if persisted.ID != "" && !validSessionID(persisted.ID) {
		return sessionRecord{}, ErrCorruptStore
	}
	return sessionRecord{id: persisted.ID, csrfHash: csrfHash, expiresAt: persisted.ExpiresAt, userID: persisted.UserID, credentialEpoch: persisted.CredentialEpoch}, nil
}

func securePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrCorruptStore
	}
	return os.Chmod(path, 0700)
}

func validatePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrCorruptStore
	}
	return nil
}

func validPasswordLength(password string) bool {
	return len(password) >= minPasswordBytes && len(password) <= maxPasswordBytes
}

func randomToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func hashSessionToken(token string) [sha256.Size]byte {
	return sha256.Sum256([]byte(token))
}

func randomSessionID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func sessionFilenameID(token string) string {
	hash := hashSessionToken(token)
	return hex.EncodeToString(hash[:])
}

func validSessionID(value string) bool {
	if len(value) != 16*2 && len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validSessionFileName(value string) bool {
	return len(value) == sha256.Size*2 && validSessionID(value)
}

func readAdminHashReadOnly(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, os.ErrNotExist
		}
		return nil, ErrStorage
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > 4096 {
		return nil, ErrCorruptStore
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, ErrStorage
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return nil, ErrCorruptStore
	}
	decoder := json.NewDecoder(io.LimitReader(f, 4096))
	decoder.DisallowUnknownFields()
	var record adminRecord
	if err := decoder.Decode(&record); err != nil || record.PasswordHash == "" {
		return nil, ErrCorruptStore
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, ErrCorruptStore
	}
	hash := []byte(record.PasswordHash)
	if _, err := bcrypt.Cost(hash); err != nil {
		return nil, ErrCorruptStore
	}
	return hash, nil
}

func atomicWrite(path string, data []byte, mode os.FileMode) (retErr error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".authn-*.tmp")
	if err != nil {
		return fmt.Errorf("%w: create temporary record", ErrStorage)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		if retErr != nil {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(mode); err != nil {
		return fmt.Errorf("%w: set record permissions", ErrStorage)
	}
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("%w: write record", ErrStorage)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("%w: sync record", ErrStorage)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("%w: close record", ErrStorage)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("%w: publish record", ErrStorage)
	}
	if err := syncDirectory(dir); err != nil {
		return fmt.Errorf("%w: sync record directory", ErrStorage)
	}
	return nil
}

// atomicCreate publishes a record only when no prior file exists. It is used
// for the administrator identity so concurrent Host/Control bootstrap calls
// cannot race through process-local mutexes and overwrite one another.
func atomicCreate(path string, data []byte, mode os.FileMode) (retErr error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".authn-create-*.tmp")
	if err != nil {
		return fmt.Errorf("%w: create temporary record", ErrStorage)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(mode); err != nil {
		return fmt.Errorf("%w: set record permissions", ErrStorage)
	}
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("%w: write record", ErrStorage)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("%w: sync record", ErrStorage)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("%w: close record", ErrStorage)
	}
	if err := os.Link(tmpPath, path); err != nil {
		return err
	}
	if err := syncDirectory(dir); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("%w: sync record directory", ErrStorage)
	}
	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
