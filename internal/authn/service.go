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
	securityDirectory       = "security"
	sessionDirectory        = "sessions"
	adminFilename           = "admin.json"
	usersFilename           = "users.json"
	bootstrapTokenFilename  = "bootstrap-token"
	maxPasswordBytes        = 72 // bcrypt's input limit.
	minPasswordBytes        = 12
	maxActiveSessions       = 1024
	defaultSessionLifetime  = 12 * time.Hour
	defaultBcryptCost       = bcrypt.DefaultCost
	maxBootstrapTokenLength = 256
	maxSessionRecordBytes   = 1024
	maxUserStoreBytes       = 1 << 20
)

var (
	ErrBootstrapUnavailable  = errors.New("authentication bootstrap is unavailable")
	ErrInvalidBootstrapToken = errors.New("invalid bootstrap token")
	ErrInvalidPassword       = errors.New("password must be between 12 and 72 bytes")
	ErrInvalidCredentials    = errors.New("invalid credentials")
	ErrUnauthenticated       = errors.New("unauthenticated")
	ErrSessionCapacity       = errors.New("session capacity reached")
	ErrNotBootstrapped       = errors.New("administrator is not configured")
	ErrUserNotFound          = errors.New("user not found")
	ErrDuplicateLogin        = errors.New("login identity already exists")
	ErrStorage               = errors.New("authentication storage failure")
	ErrCorruptStore          = errors.New("authentication store is invalid")
)

// Session is returned to the caller exactly once after login. The session
// token is omitted from JSON so callers can place it in an HttpOnly cookie;
// the CSRF token remains available for request-header validation.
type Session struct {
	Token     string    `json:"-"`
	CSRFToken string    `json:"csrf_token,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
	UserID    string    `json:"-"`
	Login     string    `json:"-"`
	Role      string    `json:"-"`
}

type adminRecord struct {
	PasswordHash string `json:"password_hash"`
}

const (
	RoleOwner        = "owner"
	UserStatusActive = "active"
)

// User is safe identity metadata. Password hashes never leave the private
// persisted user record.
type User struct {
	ID        string    `json:"user_id"`
	Login     string    `json:"login"`
	Role      string    `json:"role"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type userRecord struct {
	User
	PasswordHash string `json:"password_hash"`
}

type persistedUserStore struct {
	Version int          `json:"version"`
	Users   []userRecord `json:"users"`
}

const userStoreVersion = 1

type sessionRecord struct {
	csrfHash  [sha256.Size]byte
	expiresAt time.Time
	userID    string
}

type persistedSessionRecord struct {
	CSRFHash  string    `json:"csrf_hash"`
	ExpiresAt time.Time `json:"expires_at"`
	UserID    string    `json:"user_id,omitempty"`
}

// Service manages local users and sessions persisted as private,
// per-session records. Per-session records allow overlapping control-plane
// generations to share session validity without read-modify-write races on a
// shared session-map file.
type Service struct {
	mu sync.Mutex

	securityDir          string
	sessionsDir          string
	allowToken           bool
	removeSessionFile    func(string) error
	syncSessionDirectory func(string) error

	now        func() time.Time
	bcryptCost int
}

// Open loads authentication state and prepares first-run bootstrap if no
// administrator exists. The bootstrap token is persisted in a restricted
// file and is never returned by this API.
func Open(dataRoot string) (*Service, error) {
	return open(dataRoot, true)
}

// OpenWithoutBootstrap opens existing authentication state without creating a
// new bootstrap credential. Runtime Host uses this in recovery mode so a
// missing administrator can never silently turn a ready installation into a
// claimable one.
func OpenWithoutBootstrap(dataRoot string) (*Service, error) {
	return open(dataRoot, false)
}

func open(dataRoot string, allowToken bool) (*Service, error) {
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
		allowToken:           allowToken,
		removeSessionFile:    os.Remove,
		syncSessionDirectory: syncDirectory,
	}
	if err := s.loadOrMigrateUsers(allowToken); err != nil {
		if !allowToken && errors.Is(err, ErrCorruptStore) {
			// Recovery mode can serve its bounded UI, but corrupt identity state
			// never becomes claimable or writable.
			return s, nil
		}
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(securityDir, usersFilename)); err == nil {
		// Older session records have no user binding. Invalidate them on the
		// identity model transition instead of guessing which user owns them.
		if err := s.invalidateUnboundSessions(); err != nil {
			return nil, err
		}
		if err := removeIfExists(filepath.Join(securityDir, bootstrapTokenFilename)); err != nil {
			return nil, fmt.Errorf("%w: remove consumed bootstrap token", ErrStorage)
		}
		if err := syncDirectory(securityDir); err != nil {
			return nil, fmt.Errorf("%w: sync security directory", ErrStorage)
		}
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

// ReadSetupCode returns the existing one-time bootstrap credential for a
// local CLI. It is deliberately read-only and never rotates or creates a
// credential. Callers must not expose it through an HTTP API or logs.
func ReadSetupCode(dataRoot string) (string, error) {
	configured, err := InspectAdministrator(dataRoot)
	if err != nil {
		return "", err
	}
	if configured {
		return "", ErrBootstrapUnavailable
	}
	root, err := filepath.Abs(dataRoot)
	if err != nil {
		return "", ErrStorage
	}
	path := filepath.Join(root, securityDirectory, bootstrapTokenFilename)
	token, err := readBootstrapTokenReadOnly(path)
	if err != nil {
		return "", err
	}
	decoded, err := base64.RawURLEncoding.DecodeString(string(token))
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != string(token) {
		return "", ErrCorruptStore
	}
	return string(token), nil
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

// Bootstrap creates first owner if token matches the
// persisted first-run token. A successful call consumes the bootstrap token.
func (s *Service) Bootstrap(token, password string) error {
	if !validPasswordLength(password) {
		return ErrInvalidPassword
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.allowToken {
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

	storedToken, err := readBootstrapToken(filepath.Join(s.securityDir, bootstrapTokenFilename))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrBootstrapUnavailable
		}
		return err
	}
	providedHash := sha256.Sum256([]byte(token))
	storedHash := sha256.Sum256(storedToken)
	if subtle.ConstantTimeCompare(providedHash[:], storedHash[:]) != 1 {
		return ErrInvalidBootstrapToken
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost)
	if err != nil {
		return fmt.Errorf("%w: hash administrator password", ErrStorage)
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
			return ErrBootstrapUnavailable
		}
		return err
	}
	if err := removeIfExists(filepath.Join(s.securityDir, bootstrapTokenFilename)); err != nil {
		return fmt.Errorf("%w: consume bootstrap token", ErrStorage)
	}
	if err := syncDirectory(s.securityDir); err != nil {
		return fmt.Errorf("%w: sync consumed bootstrap token", ErrStorage)
	}
	return nil
}

// NeedsBootstrap reports whether the administrator has not yet been set up.
// BootstrapTokenRelativePath returns the stable, data-root-relative path
// where the first-run token is stored. It never returns the token itself or
// an absolute filesystem path.
func (s *Service) BootstrapTokenRelativePath() string {
	return filepath.ToSlash(filepath.Join(securityDirectory, bootstrapTokenFilename))
}

// Login preserves password-only owner login for existing browser clients.
func (s *Service) Login(password string) (Session, error) {
	return s.LoginAs("owner", password)
}

// LoginAs verifies a login identity and creates a durable, user-bound session.
func (s *Service) LoginAs(login, password string) (Session, error) {
	if !validPasswordLength(password) {
		return Session{}, ErrInvalidCredentials
	}
	user, err := s.VerifyCredentials(login, password)
	if err != nil {
		if errors.Is(err, ErrNotBootstrapped) {
			return Session{}, ErrNotBootstrapped
		}
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
	now := s.now()
	session := Session{Token: token, CSRFToken: csrf, ExpiresAt: now.Add(defaultSessionLifetime), UserID: user.ID, Login: user.Login, Role: user.Role}
	csrfHash := sha256.Sum256([]byte(csrf))
	s.mu.Lock()
	defer s.mu.Unlock()
	active, err := s.countAndCleanSessions(now)
	if err != nil {
		return Session{}, err
	}
	if active >= maxActiveSessions {
		return Session{}, ErrSessionCapacity
	}
	if err := s.writeSession(token, sessionRecord{csrfHash: csrfHash, expiresAt: session.ExpiresAt, userID: user.ID}); err != nil {
		return Session{}, err
	}
	// A second control process may have admitted a login concurrently. Keep the
	// historical capacity bound best-effort across processes without a shared
	// mutable map: if this admission crossed the bound, revoke this new session.
	active, err = s.countAndCleanSessions(now)
	if err != nil {
		s.removeSession(token)
		return Session{}, err
	}
	if active > maxActiveSessions {
		s.removeSession(token)
		return Session{}, ErrSessionCapacity
	}
	return session, nil
}

// Authenticate validates a session token. Returned Session intentionally
// omits the token and CSRF secret; those are only available from Login.
func (s *Service) Authenticate(token string) (Session, error) {
	if token == "" {
		return Session{}, ErrUnauthenticated
	}
	record, err := s.readSession(token)
	if err != nil {
		return Session{}, ErrUnauthenticated
	}
	if !s.now().Before(record.expiresAt) || !validUserID(record.userID) {
		s.removeSession(token)
		return Session{}, ErrUnauthenticated
	}
	user, err := s.GetUser(record.userID)
	if err != nil || user.Status != UserStatusActive {
		s.removeSession(token)
		return Session{}, ErrUnauthenticated
	}
	return Session{ExpiresAt: record.expiresAt, UserID: user.ID, Login: user.Login, Role: user.Role}, nil
}

// Logout durably revokes a session. Unknown and empty tokens are harmless.
func (s *Service) Logout(token string) error {
	if token == "" {
		return nil
	}
	return s.removeSession(token)
}

// ValidCSRF compares the supplied value with the CSRF token bound to a live
// session token. Both values are hashed before constant-time comparison.
func (s *Service) ValidCSRF(sessionToken, supplied string) bool {
	if sessionToken == "" {
		return false
	}
	suppliedHash := sha256.Sum256([]byte(supplied))
	record, err := s.readSession(sessionToken)
	if err != nil {
		return false
	}
	if !s.now().Before(record.expiresAt) {
		s.removeSession(sessionToken)
		return false
	}
	return validUserID(record.userID) && subtle.ConstantTimeCompare(record.csrfHash[:], suppliedHash[:]) == 1
}

func (s *Service) readSession(token string) (sessionRecord, error) {
	if token == "" {
		return sessionRecord{}, os.ErrNotExist
	}
	if err := validatePrivateDirectory(s.sessionsDir); err != nil {
		return sessionRecord{}, err
	}
	return readSessionRecordAt(s.sessionPath(token))
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
	persisted := persistedSessionRecord{CSRFHash: hex.EncodeToString(record.csrfHash[:]), ExpiresAt: record.expiresAt.UTC(), UserID: record.userID}
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
		if !now.Before(record.expiresAt) || !validUserID(record.userID) {
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
	return sessionRecord{csrfHash: csrfHash, expiresAt: persisted.ExpiresAt, userID: persisted.UserID}, nil
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

func ensureBootstrapToken(securityDir string) error {
	path := filepath.Join(securityDir, bootstrapTokenFilename)
	token, err := randomToken()
	if err != nil {
		return fmt.Errorf("%w: generate bootstrap token", ErrStorage)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		if _, readErr := readBootstrapToken(path); readErr != nil {
			return readErr
		}
		if chmodErr := os.Chmod(path, 0600); chmodErr != nil {
			return fmt.Errorf("%w: secure bootstrap token", ErrStorage)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: create bootstrap token", ErrStorage)
	}
	cleanup := true
	defer func() {
		_ = f.Close()
		if cleanup {
			_ = os.Remove(path)
		}
	}()
	if _, err := io.WriteString(f, token); err != nil {
		return fmt.Errorf("%w: write bootstrap token", ErrStorage)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("%w: sync bootstrap token", ErrStorage)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("%w: close bootstrap token", ErrStorage)
	}
	if err := syncDirectory(securityDir); err != nil {
		return fmt.Errorf("%w: sync security directory", ErrStorage)
	}
	cleanup = false
	return nil
}

func readBootstrapToken(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("%w: inspect bootstrap token", ErrStorage)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maxBootstrapTokenLength {
		return nil, ErrCorruptStore
	}
	if err := os.Chmod(path, 0600); err != nil {
		return nil, fmt.Errorf("%w: secure bootstrap token", ErrStorage)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: read bootstrap token", ErrStorage)
	}
	if len(data) == 0 || len(data) > maxBootstrapTokenLength {
		return nil, ErrCorruptStore
	}
	return data, nil
}

func readBootstrapTokenReadOnly(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, os.ErrNotExist
		}
		return nil, ErrStorage
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maxBootstrapTokenLength {
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
	data, err := io.ReadAll(io.LimitReader(f, maxBootstrapTokenLength+1))
	if err != nil || len(data) == 0 || len(data) > maxBootstrapTokenLength {
		return nil, ErrCorruptStore
	}
	return data, nil
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
