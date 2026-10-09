package authn

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const testPassword = "correct-horse-battery"

func openForTest(t *testing.T, root string) (*Service, string) {
	t.Helper()
	service, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	service.bcryptCost = bcrypt.MinCost
	bootstrapPath := filepath.Join(root, filepath.FromSlash(service.BootstrapTokenRelativePath()))
	token, err := os.ReadFile(bootstrapPath)
	if err != nil {
		t.Fatalf("read test bootstrap token: %v", err)
	}
	return service, string(token)
}

func bootstrapForTest(t *testing.T, service *Service, token string) {
	t.Helper()
	if err := service.Bootstrap(token, testPassword); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
}

func TestFirstRunTokenPermissionsPersistenceAndNoDisclosure(t *testing.T) {
	root := t.TempDir()
	service, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if !service.NeedsBootstrap() {
		t.Fatal("new installation does not need bootstrap")
	}
	if got := service.BootstrapTokenRelativePath(); got != "security/bootstrap-token" {
		t.Fatalf("bootstrap token path = %q", got)
	}
	tokenPath := filepath.Join(root, filepath.FromSlash(service.BootstrapTokenRelativePath()))
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(token) == 0 || strings.Contains(service.BootstrapTokenRelativePath(), string(token)) {
		t.Fatal("token missing or disclosed by path API")
	}
	for path, want := range map[string]os.FileMode{
		filepath.Join(root, "security"): 0700,
		tokenPath:                       0600,
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("%s mode = %#o, want %#o", filepath.Base(path), got, want)
		}
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	reopened.bcryptCost = bcrypt.MinCost
	tokenAgain, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(tokenAgain) != string(token) {
		t.Fatal("reopen rotated the persisted bootstrap token")
	}
}

func TestBootstrapRejectsWrongTokenConsumesTokenAndRejectsReplay(t *testing.T) {
	root := t.TempDir()
	service, token := openForTest(t, root)
	tokenPath := filepath.Join(root, filepath.FromSlash(service.BootstrapTokenRelativePath()))
	wrong := "wrong bootstrap token"
	if err := service.Bootstrap(wrong, testPassword); !errors.Is(err, ErrInvalidBootstrapToken) {
		t.Fatalf("wrong token error = %v", err)
	}
	if err := service.Bootstrap(token, testPassword); err != nil {
		t.Fatalf("valid bootstrap: %v", err)
	}
	if service.NeedsBootstrap() {
		t.Fatal("bootstrap did not install administrator")
	}
	if _, err := os.Stat(tokenPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("bootstrap token still exists: %v", err)
	}
	if err := service.Bootstrap(token, testPassword); !errors.Is(err, ErrBootstrapUnavailable) {
		t.Fatalf("bootstrap replay error = %v", err)
	}
	if strings.Contains(ErrInvalidBootstrapToken.Error(), token) || strings.Contains(ErrInvalidBootstrapToken.Error(), testPassword) {
		t.Fatal("auth error disclosed sensitive input")
	}
}

func TestConcurrentBootstrapCreatesOneOwner(t *testing.T) {
	root := t.TempDir()
	first, token := openForTest(t, root)
	second, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	first.bcryptCost, second.bcryptCost = bcrypt.MinCost, bcrypt.MinCost
	services := []*Service{first, second}
	const callers = 12
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes, rejected := 0, 0
	for i := 0; i < callers; i++ {
		service := services[i%len(services)]
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := service.Bootstrap(token, testPassword)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				successes++
			case errors.Is(err, ErrBootstrapUnavailable):
				rejected++
			default:
				t.Errorf("concurrent bootstrap: %v", err)
			}
		}()
	}
	wg.Wait()
	store, err := readUserStore(filepath.Join(root, securityDirectory, usersFilename))
	if err != nil {
		t.Fatal(err)
	}
	if successes != 1 || rejected != callers-1 || len(store.Users) != 1 {
		t.Fatalf("success=%d rejected=%d owners=%d", successes, rejected, len(store.Users))
	}
}

func TestReadSetupCodeIsReadOnlyAndRefusesAfterClaim(t *testing.T) {
	root := t.TempDir()
	service, token := openForTest(t, root)
	got, err := ReadSetupCode(root)
	if err != nil || got != token {
		t.Fatalf("ReadSetupCode = %q, %v; want existing token", got, err)
	}
	if err := service.Bootstrap(token, testPassword); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadSetupCode(root); got != "" || !errors.Is(err, ErrBootstrapUnavailable) {
		t.Fatalf("claimed ReadSetupCode = %q, %v; want unavailable", got, err)
	}
}

func TestAuthenticationInstancesRefreshAdministratorAfterCrossProcessBootstrap(t *testing.T) {
	root := t.TempDir()
	host, token := openForTest(t, root)
	control, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	control.bcryptCost = bcrypt.MinCost
	if err := control.Bootstrap(token, testPassword); err != nil {
		t.Fatal(err)
	}
	configured, err := host.AdministratorConfigured()
	if err != nil || !configured {
		t.Fatalf("Host administrator inspection configured=%v err=%v", configured, err)
	}
	if host.NeedsBootstrap() {
		t.Fatal("Host auth instance still reports bootstrap required")
	}
	if _, err := host.Login(testPassword); err != nil {
		t.Fatalf("Host auth instance could not log in after Control bootstrap: %v", err)
	}
	if err := host.Bootstrap(token, "another-valid-password"); !errors.Is(err, ErrBootstrapUnavailable) {
		t.Fatalf("stale Host instance bootstrap replay error=%v", err)
	}
}

func TestOpenWithoutBootstrapNeverCreatesReplacementToken(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, securityDirectory), 0700); err != nil {
		t.Fatal(err)
	}
	service, err := OpenWithoutBootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Bootstrap("no-token", testPassword); !errors.Is(err, ErrBootstrapUnavailable) {
		t.Fatalf("recovery-only auth bootstrap error=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, securityDirectory, bootstrapTokenFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovery auth created bootstrap token: %v", err)
	}
}

func TestOpenWithoutBootstrapServesFailClosedOnCorruptAdmin(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, securityDirectory), 0700); err != nil {
		t.Fatal(err)
	}
	adminPath := filepath.Join(root, securityDirectory, adminFilename)
	if err := os.WriteFile(adminPath, []byte(`{not-json`), 0600); err != nil {
		t.Fatal(err)
	}
	service, err := OpenWithoutBootstrap(root)
	if err != nil {
		t.Fatalf("recovery auth should still start read-only: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, securityDirectory, bootstrapTokenFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt admin generated setup token: %v", err)
	}
	if _, err := service.Login(testPassword); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("login against corrupt admin error=%v", err)
	}
}

func TestPasswordPolicyAndBcryptPersistence(t *testing.T) {
	root := t.TempDir()
	service, token := openForTest(t, root)
	for _, password := range []string{"short-pass", strings.Repeat("x", 73)} {
		if err := service.Bootstrap(token, password); !errors.Is(err, ErrInvalidPassword) {
			t.Errorf("Bootstrap(%d bytes) error = %v", len(password), err)
		}
	}
	bootstrapForTest(t, service, token)
	data, err := os.ReadFile(filepath.Join(root, "security", usersFilename))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), testPassword) {
		t.Fatal("plaintext password persisted")
	}
	var store persistedUserStore
	if err := json.Unmarshal(data, &store); err != nil {
		t.Fatal(err)
	}
	if len(store.Users) != 1 || store.Users[0].Role != RoleOwner || store.Users[0].Login != "owner" || !validUserID(store.Users[0].ID) {
		t.Fatalf("bootstrap owner identity invalid: %#v", store.Users)
	}
	if cost, err := bcrypt.Cost([]byte(store.Users[0].PasswordHash)); err != nil || cost != bcrypt.MinCost {
		t.Fatalf("persisted password hash invalid: cost=%d err=%v", cost, err)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(store.Users[0].PasswordHash), []byte(testPassword)); err != nil {
		t.Fatalf("persisted bcrypt hash does not verify: %v", err)
	}
	if _, err := service.Login("wrong-password-123"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("wrong password error = %v", err)
	}
	if _, err := service.Login(strings.Repeat("x", 73)); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("overlong login error = %v", err)
	}
}

func TestLegacyAdministratorMigratesHashAndInvalidatesUnboundSessions(t *testing.T) {
	root := t.TempDir()
	security := filepath.Join(root, securityDirectory)
	if err := os.MkdirAll(filepath.Join(security, sessionDirectory), 0700); err != nil {
		t.Fatal(err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(testPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	adminData, err := json.Marshal(adminRecord{PasswordHash: string(hash)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(security, adminFilename), append(adminData, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	oldToken := "legacy-unbound-session-token"
	oldCSRF := make([]byte, sha256.Size)
	oldCSRF[0] = 7
	legacySession, err := json.Marshal(persistedSessionRecord{CSRFHash: hex.EncodeToString(oldCSRF), ExpiresAt: time.Now().Add(time.Hour).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	oldSessionHash := hashSessionToken(oldToken)
	oldSessionPath := filepath.Join(security, sessionDirectory, hex.EncodeToString(oldSessionHash[:]))
	if err := os.WriteFile(oldSessionPath, append(legacySession, '\n'), 0600); err != nil {
		t.Fatal(err)
	}

	service, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := service.FindByLogin("owner")
	if err != nil {
		t.Fatal(err)
	}
	store, err := readUserStore(filepath.Join(security, usersFilename))
	if err != nil {
		t.Fatal(err)
	}
	if len(store.Users) != 1 || string(store.Users[0].PasswordHash) != string(hash) {
		t.Fatal("legacy bcrypt credential was not copied exactly")
	}
	if _, err := os.Stat(oldSessionPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unbound legacy session was not invalidated: %v", err)
	}
	if _, err := service.Authenticate(oldToken); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("legacy session authenticated after migration: %v", err)
	}
	if _, err := service.Login(testPassword); err != nil {
		t.Fatalf("migrated owner could not log in: %v", err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	ownerAgain, err := reopened.FindByLogin("owner")
	if err != nil || ownerAgain.ID != owner.ID {
		t.Fatalf("migration was not idempotent: owner=%q reopened=%q err=%v", owner.ID, ownerAgain.ID, err)
	}
}

func TestTwoUsersHaveIndependentSessionsCSRFLogoutAndRestartIdentity(t *testing.T) {
	root := t.TempDir()
	service, token := openForTest(t, root)
	bootstrapForTest(t, service, token)
	ownerSession, err := service.Login(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.CreateUser("operator-b", "another-valid-password", RoleOwner)
	if err != nil {
		t.Fatal(err)
	}
	secondSession, err := service.LoginAs(second.Login, "another-valid-password")
	if err != nil {
		t.Fatal(err)
	}
	if ownerSession.UserID == secondSession.UserID || ownerSession.UserID == "" || secondSession.UserID != second.ID {
		t.Fatalf("sessions do not bind distinct identities: owner=%q second=%q", ownerSession.UserID, secondSession.UserID)
	}
	if service.ValidCSRF(ownerSession.Token, secondSession.CSRFToken) || service.ValidCSRF(secondSession.Token, ownerSession.CSRFToken) {
		t.Fatal("CSRF token crossed user session")
	}
	ownerPrincipal := Principal{UserID: ownerSession.UserID, Login: ownerSession.Login, Role: ownerSession.Role}
	secondPrincipal := Principal{UserID: secondSession.UserID, Login: secondSession.Login, Role: secondSession.Role}
	ownerFromContext, ok := PrincipalFromContext(WithPrincipal(context.Background(), ownerPrincipal))
	if !ok || ownerFromContext.UserID != ownerPrincipal.UserID {
		t.Fatal("owner principal context was not preserved")
	}
	secondFromContext, ok := PrincipalFromContext(WithPrincipal(context.Background(), secondPrincipal))
	if !ok || secondFromContext.UserID != secondPrincipal.UserID {
		t.Fatal("second user principal context was not preserved")
	}
	if !HasPermission(ownerPrincipal, PermissionAuditRead) || !HasPermission(secondPrincipal, PermissionRecordingControl) || HasPermission(secondPrincipal, Permission("unknown.permission")) {
		t.Fatal("owner permission policy is not explicit")
	}
	service.Logout(ownerSession.Token)
	if _, err := service.Authenticate(ownerSession.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("logged-out owner session remains valid: %v", err)
	}
	if _, err := service.Authenticate(secondSession.Token); err != nil {
		t.Fatalf("logging out owner revoked another user's session: %v", err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	restartedSession, err := reopened.Authenticate(secondSession.Token)
	if err != nil || restartedSession.UserID != second.ID || restartedSession.Login != second.Login {
		t.Fatalf("session identity did not survive restart: %#v err=%v", restartedSession, err)
	}
	if _, err := reopened.LoginAs(second.Login, "wrong-password-123"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("bad second-user password error=%v", err)
	}
}

func TestRequirePermissionUsesContextPrincipalNotClientHeader(t *testing.T) {
	called := false
	protected := RequirePermission(PermissionAuditRead, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	request := httptest.NewRequest(http.MethodGet, "/api/audit", nil)
	request.Header.Set("X-User-ID", "usr-00000000000000000000000000000000")
	response := httptest.NewRecorder()
	protected.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || called {
		t.Fatalf("client header supplied principal: status=%d called=%v", response.Code, called)
	}

	principal := Principal{UserID: "usr-00000000000000000000000000000000", Login: "owner", Role: RoleOwner}
	request = request.WithContext(WithPrincipal(request.Context(), principal))
	response = httptest.NewRecorder()
	protected.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !called {
		t.Fatalf("owner permission rejected: status=%d called=%v", response.Code, called)
	}
	called = false
	protected = RequirePermission(Permission("unknown.permission"), http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	response = httptest.NewRecorder()
	protected.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || called {
		t.Fatalf("unknown permission accepted: status=%d called=%v", response.Code, called)
	}
}

func TestLoginAuthenticateLogoutAndCSRF(t *testing.T) {
	root := t.TempDir()
	service, token := openForTest(t, root)
	bootstrapForTest(t, service, token)
	session, err := service.Login(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	if len(session.Token) < 40 || len(session.CSRFToken) < 40 || !session.ExpiresAt.After(time.Now()) {
		t.Fatalf("invalid session response: %#v", session)
	}
	if _, err := service.Authenticate(session.Token); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if !service.ValidCSRF(session.Token, session.CSRFToken) {
		t.Fatal("valid CSRF token rejected")
	}
	if service.ValidCSRF(session.Token, session.CSRFToken+"x") || service.ValidCSRF(session.Token, "") {
		t.Fatal("invalid CSRF token accepted")
	}
	service.Logout(session.Token)
	if _, err := service.Authenticate(session.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("Authenticate after logout error = %v", err)
	}
	if service.ValidCSRF(session.Token, session.CSRFToken) {
		t.Fatal("CSRF validation succeeded after logout")
	}
	encoded, err := json.Marshal(session)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), session.Token) || strings.Contains(string(encoded), session.UserID) || strings.Contains(string(encoded), session.Login) || !strings.Contains(string(encoded), session.CSRFToken) {
		t.Fatal("session JSON must hide identity and cookie token, and expose only CSRF token")
	}
}

func TestLogoutRequiresDurableSessionRevocation(t *testing.T) {
	t.Run("remove failure preserves valid session", func(t *testing.T) {
		root := t.TempDir()
		service, token := openForTest(t, root)
		bootstrapForTest(t, service, token)
		session, err := service.Login(testPassword)
		if err != nil {
			t.Fatal(err)
		}
		remove := service.removeSessionFile
		service.removeSessionFile = func(string) error { return errors.New("/private/path must not escape") }
		err = service.Logout(session.Token)
		if !errors.Is(err, ErrStorage) || strings.Contains(err.Error(), "/private/path") {
			t.Fatalf("Logout error=%v, want safe storage error", err)
		}
		if _, err := service.Authenticate(session.Token); err != nil {
			t.Fatalf("session should remain valid after failed removal: %v", err)
		}
		service.removeSessionFile = remove
		if err := service.Logout(session.Token); err != nil {
			t.Fatalf("retry Logout: %v", err)
		}
		if _, err := service.Authenticate(session.Token); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("session remains valid after durable retry: %v", err)
		}
	})

	t.Run("directory sync failure is reported", func(t *testing.T) {
		root := t.TempDir()
		service, token := openForTest(t, root)
		bootstrapForTest(t, service, token)
		session, err := service.Login(testPassword)
		if err != nil {
			t.Fatal(err)
		}
		syncDir := service.syncSessionDirectory
		service.syncSessionDirectory = func(string) error { return errors.New("/private/path must not escape") }
		err = service.Logout(session.Token)
		if !errors.Is(err, ErrStorage) || strings.Contains(err.Error(), "/private/path") {
			t.Fatalf("Logout error=%v, want safe durability error", err)
		}
		if _, err := service.Authenticate(session.Token); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("removed session remained valid in current process: %v", err)
		}
		service.syncSessionDirectory = syncDir
		if err := service.Logout(session.Token); err != nil {
			t.Fatalf("retry Logout after uncertain durability: %v", err)
		}
	})
}

func TestSessionExpiryAndCrossProcessPersistence(t *testing.T) {
	root := t.TempDir()
	service, token := openForTest(t, root)
	bootstrapForTest(t, service, token)
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return clock }
	session, err := service.Login(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	reopened.now = func() time.Time { return clock }
	if _, err := reopened.Authenticate(session.Token); err != nil {
		t.Fatalf("session did not survive control-plane replacement: %v", err)
	}
	if !reopened.ValidCSRF(session.Token, session.CSRFToken) {
		t.Fatal("CSRF validation did not survive control-plane replacement")
	}
	clock = clock.Add(defaultSessionLifetime + time.Second)
	if _, err := reopened.Authenticate(session.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("expired session error = %v", err)
	}
	if _, err := os.Stat(service.sessionPath(session.Token)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired session record remains: %v", err)
	}

	service.now = time.Now
	restartSession, err := service.Login(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	reopenedAgain, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if reopenedAgain.NeedsBootstrap() {
		t.Fatal("restart lost configured administrator")
	}
	if _, err := reopenedAgain.Authenticate(restartSession.Token); err != nil {
		t.Fatalf("new session did not survive another control-plane replacement: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "security", bootstrapTokenFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("consumed token exists after restart: %v", err)
	}
}

func TestLogoutInOneServiceRevokesOtherServiceSession(t *testing.T) {
	root := t.TempDir()
	first, bootstrapToken := openForTest(t, root)
	bootstrapForTest(t, first, bootstrapToken)
	second, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	second.bcryptCost = bcrypt.MinCost
	session, err := first.Login(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Authenticate(session.Token); err != nil {
		t.Fatalf("second service could not authenticate session: %v", err)
	}
	second.Logout(session.Token)
	if _, err := first.Authenticate(session.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("first service retained logged-out session: %v", err)
	}
	if first.ValidCSRF(session.Token, session.CSRFToken) {
		t.Fatal("first service accepted CSRF after another service logged out")
	}
}

func TestSessionRecordPermissionsAndDoesNotPersistSecrets(t *testing.T) {
	root := t.TempDir()
	service, bootstrapToken := openForTest(t, root)
	bootstrapForTest(t, service, bootstrapToken)
	session, err := service.Login(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, securityDirectory, sessionDirectory)
	if got := mustMode(t, dir); got != 0700 {
		t.Fatalf("sessions directory mode = %#o, want 0700", got)
	}
	path := service.sessionPath(session.Token)
	if filepath.Base(path) != fmt.Sprintf("%x", sha256.Sum256([]byte(session.Token))) {
		t.Fatalf("session filename is not token SHA-256: %q", filepath.Base(path))
	}
	if got := mustMode(t, path); got != 0600 {
		t.Fatalf("session file mode = %#o, want 0600", got)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), session.Token) || strings.Contains(string(data), session.CSRFToken) {
		t.Fatal("session file contains raw session or CSRF token")
	}
	if strings.Contains(filepath.Base(path), session.Token) {
		t.Fatal("session token disclosed in filename")
	}
}

func TestMalformedAndSymlinkSessionRecordsAreRejected(t *testing.T) {
	root := t.TempDir()
	service, bootstrapToken := openForTest(t, root)
	bootstrapForTest(t, service, bootstrapToken)
	valid, err := service.Login(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	malformedToken, err := randomToken()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(service.sessionPath(malformedToken), []byte(`{"csrf_hash":"nope","expires_at":"not-time"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Authenticate(malformedToken); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("malformed session accepted: %v", err)
	}
	if service.ValidCSRF(malformedToken, "anything") {
		t.Fatal("malformed session passed CSRF validation")
	}

	linkedToken, err := randomToken()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(service.sessionPath(valid.Token), service.sessionPath(linkedToken)); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := service.Authenticate(linkedToken); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("symlink session record accepted: %v", err)
	}
	if service.ValidCSRF(linkedToken, valid.CSRFToken) {
		t.Fatal("symlink session passed CSRF validation")
	}
}

func TestConcurrentSessionCapacityAndExpiredCleanup(t *testing.T) {
	root := t.TempDir()
	service, token := openForTest(t, root)
	bootstrapForTest(t, service, token)
	now := time.Now()
	service.now = func() time.Time { return now }
	for i := 0; i < maxActiveSessions-1; i++ {
		path := filepath.Join(service.sessionsDir, fmt.Sprintf("%064x", i+1))
		writeTestSessionRecord(t, path, now.Add(time.Hour))
	}

	const callers = 16
	var wg sync.WaitGroup
	wg.Add(callers)
	var successes, capacityErrors int
	var resultMu sync.Mutex
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			_, err := service.Login(testPassword)
			resultMu.Lock()
			defer resultMu.Unlock()
			switch {
			case err == nil:
				successes++
			case errors.Is(err, ErrSessionCapacity):
				capacityErrors++
			default:
				t.Errorf("Login: %v", err)
			}
		}()
	}
	wg.Wait()
	entries, err := os.ReadDir(service.sessionsDir)
	if err != nil {
		t.Fatal(err)
	}
	if successes != 1 || capacityErrors != callers-1 || len(entries) != maxActiveSessions {
		t.Fatalf("success=%d capacity=%d session records=%d", successes, capacityErrors, len(entries))
	}

	for _, entry := range entries {
		writeTestSessionRecord(t, filepath.Join(service.sessionsDir, entry.Name()), now.Add(-time.Second))
	}
	if _, err := service.Login(testPassword); err != nil {
		t.Fatalf("login after expired-session cleanup: %v", err)
	}
	entries, err = os.ReadDir(service.sessionsDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("lazy cleanup left %d session records", len(entries))
	}
}

func writeTestSessionRecord(t *testing.T, path string, expiresAt time.Time) {
	t.Helper()
	csrfHash := make([]byte, sha256.Size)
	csrfHash[0] = 1
	record := persistedSessionRecord{CSRFHash: hex.EncodeToString(csrfHash), ExpiresAt: expiresAt.UTC(), UserID: "usr-00000000000000000000000000000000"}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func mustMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}

func TestSessionCookieFlagsAndExtraction(t *testing.T) {
	session := Session{Token: "session-secret", ExpiresAt: time.Now().Add(time.Hour)}
	for _, tc := range []struct {
		name       string
		tls        bool
		force      bool
		wantSecure bool
	}{
		{name: "plain"},
		{name: "TLS", tls: true, wantSecure: true},
		{name: "forced", force: true, wantSecure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "http://example.test/", nil)
			if tc.tls {
				request.TLS = &tls.ConnectionState{}
			}
			recorder := httptest.NewRecorder()
			SetSessionCookie(recorder, request, session, tc.force)
			response := recorder.Result()
			cookie, err := responseCookie(response, SessionCookieName)
			if err != nil {
				t.Fatal(err)
			}
			if cookie.Value != session.Token || cookie.Path != "/" || !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.Secure != tc.wantSecure || cookie.MaxAge <= 0 {
				t.Fatalf("cookie attributes: %#v", cookie)
			}
			if got := SessionToken(requestWithCookie(request, cookie)); got != session.Token {
				t.Fatalf("SessionToken = %q", got)
			}
			clear := httptest.NewRecorder()
			ClearSessionCookie(clear, request, tc.force)
			cleared, err := responseCookie(clear.Result(), SessionCookieName)
			if err != nil {
				t.Fatal(err)
			}
			if cleared.MaxAge != -1 || !cleared.HttpOnly || cleared.Secure != tc.wantSecure || cleared.SameSite != http.SameSiteStrictMode {
				t.Fatalf("clear cookie attributes: %#v", cleared)
			}
		})
	}
}

func requestWithCookie(request *http.Request, cookie *http.Cookie) *http.Request {
	clone := request.Clone(request.Context())
	clone.AddCookie(cookie)
	return clone
}

func responseCookie(response *http.Response, name string) (*http.Cookie, error) {
	for _, cookie := range response.Cookies() {
		if cookie.Name == name {
			return cookie, nil
		}
	}
	return nil, http.ErrNoCookie
}
