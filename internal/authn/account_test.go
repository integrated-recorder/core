package authn

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func TestChangePasswordAdvancesEpochAndInvalidatesAllSessionsAcrossRestart(t *testing.T) {
	root := t.TempDir()
	service, token := openForTest(t, root)
	bootstrapForTest(t, service, token)
	service.bcryptCost = bcrypt.MinCost
	owner, err := service.FindByLogin("owner")
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.Login(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Login(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.ChangePassword(owner.ID, "wrong-current-password", "new-account-password"); !errors.Is(err, ErrCurrentPassword) {
		t.Fatalf("wrong current password error = %v", err)
	}
	if err := service.ChangePassword(owner.ID, testPassword, "short"); !errors.Is(err, ErrInvalidPassword) {
		t.Fatalf("invalid new password error = %v", err)
	}
	service.removeSessionFile = func(string) error { return errors.New("injected cleanup failure") }
	if err := service.ChangePassword(owner.ID, testPassword, "new-account-password"); err != nil {
		t.Fatalf("ChangePassword: %v", err)
	}
	store, err := readUserStore(filepath.Join(root, securityDirectory, usersFilename))
	if err != nil {
		t.Fatal(err)
	}
	if store.Users[0].CredentialEpoch != 2 {
		t.Fatalf("credential epoch=%d, want 2", store.Users[0].CredentialEpoch)
	}
	if _, err := os.Stat(service.sessionPath(first.Token)); err != nil {
		t.Fatalf("injected stale session file missing: %v", err)
	}
	if _, err := service.Authenticate(first.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("old current session remained valid: %v", err)
	}
	if service.ValidCSRF(second.Token, second.CSRFToken) {
		t.Fatal("old CSRF/session pair remained valid")
	}
	if _, err := service.Authenticate(second.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("other old session remained valid: %v", err)
	}

	reopened, err := OpenWithoutBootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	reopened.bcryptCost = bcrypt.MinCost
	if _, err := reopened.Authenticate(first.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("stale session after restart remained valid: %v", err)
	}
	if _, err := reopened.Login(testPassword); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("old password login error = %v", err)
	}
	if _, err := reopened.Login("new-account-password"); err != nil {
		t.Fatalf("new password login: %v", err)
	}
}

func TestConcurrentPasswordChangesSerializeAcrossServices(t *testing.T) {
	root := t.TempDir()
	first, token := openForTest(t, root)
	bootstrapForTest(t, first, token)
	second, err := OpenWithoutBootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	first.bcryptCost, second.bcryptCost = bcrypt.MinCost, bcrypt.MinCost
	owner, err := first.FindByLogin("owner")
	if err != nil {
		t.Fatal(err)
	}
	oldSession, err := first.Login(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	services := []*Service{first, second}
	passwords := []string{"first-new-password", "second-new-password"}
	var wg sync.WaitGroup
	results := make([]error, len(services))
	for i := range services {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = services[i].ChangePassword(owner.ID, testPassword, passwords[i])
		}(i)
	}
	wg.Wait()
	successes := 0
	validPassword := ""
	for i, err := range results {
		if err == nil {
			successes++
			validPassword = passwords[i]
		} else if !errors.Is(err, ErrCurrentPassword) {
			t.Fatalf("concurrent change error=%v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent password changes succeeded %d times", successes)
	}
	if _, err := second.Authenticate(oldSession.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("old session remained valid: %v", err)
	}
	if _, err := first.Login(validPassword); err != nil {
		t.Fatalf("winning password cannot log in: %v", err)
	}
}

func TestCredentialAndSessionRacesAreLinearizableAcrossServices(t *testing.T) {
	root := t.TempDir()
	first, token := openForTest(t, root)
	bootstrapForTest(t, first, token)
	first.bcryptCost = bcrypt.MinCost
	second, err := OpenWithoutBootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	second.bcryptCost = bcrypt.MinCost

	owner, err := first.FindByLogin("owner")
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var loginSession Session
	var loginErr, changeErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		loginSession, loginErr = first.Login(testPassword)
	}()
	go func() {
		defer wg.Done()
		<-start
		changeErr = second.ChangePassword(owner.ID, testPassword, "raced-new-password")
	}()
	close(start)
	wg.Wait()
	if changeErr != nil {
		t.Fatalf("password change racing login: %v", changeErr)
	}
	if loginErr != nil && !errors.Is(loginErr, ErrInvalidCredentials) {
		t.Fatalf("login racing password change: %v", loginErr)
	}
	if loginErr == nil {
		if _, err := second.Authenticate(loginSession.Token); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("pre-change racing login session remained valid: %v", err)
		}
	}
	if _, err := first.Login("raced-new-password"); err != nil {
		t.Fatalf("new password after login race: %v", err)
	}

	oldSession, err := first.Login("raced-new-password")
	if err != nil {
		t.Fatal(err)
	}
	start = make(chan struct{})
	var authErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, authErr = first.Authenticate(oldSession.Token)
	}()
	go func() {
		defer wg.Done()
		<-start
		changeErr = second.ChangePassword(owner.ID, "raced-new-password", "race-final-password")
	}()
	close(start)
	wg.Wait()
	if changeErr != nil {
		t.Fatalf("password change racing authenticate: %v", changeErr)
	}
	if authErr != nil && !errors.Is(authErr, ErrUnauthenticated) {
		t.Fatalf("authenticate racing password change: %v", authErr)
	}
	if _, err := first.Authenticate(oldSession.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("session racing password change remained valid: %v", err)
	}

	current, err := first.Login("race-final-password")
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := first.ListSessions(owner.ID, current.Token)
	if err != nil || len(sessions) != 1 {
		t.Fatalf("session listing before revoke race=%+v err=%v", sessions, err)
	}
	start = make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, authErr = second.Authenticate(current.Token)
	}()
	go func() {
		defer wg.Done()
		<-start
		_, changeErr = first.RevokeSession(owner.ID, sessions[0].ID, current.Token)
	}()
	close(start)
	wg.Wait()
	if changeErr != nil {
		t.Fatalf("session revoke racing authenticate: %v", changeErr)
	}
	if authErr != nil && !errors.Is(authErr, ErrUnauthenticated) {
		t.Fatalf("authenticate racing session revoke: %v", authErr)
	}
	if _, err := first.Authenticate(current.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revoked session remained valid after race: %v", err)
	}

	member, err := first.CreateUser("disable-race", "disable-race-password", RoleMember)
	if err != nil {
		t.Fatal(err)
	}
	start = make(chan struct{})
	wg.Add(2)
	var memberSession Session
	var memberLoginErr, disableErr error
	go func() {
		defer wg.Done()
		<-start
		memberSession, memberLoginErr = first.LoginAs(member.Login, "disable-race-password")
	}()
	go func() {
		defer wg.Done()
		<-start
		_, disableErr = second.SetUserEnabled(member.ID, false)
	}()
	close(start)
	wg.Wait()
	if disableErr != nil {
		t.Fatalf("disable racing login: %v", disableErr)
	}
	if memberLoginErr != nil && !errors.Is(memberLoginErr, ErrInvalidCredentials) {
		t.Fatalf("login racing disable: %v", memberLoginErr)
	}
	if memberLoginErr == nil {
		if _, err := first.Authenticate(memberSession.Token); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("disabled account racing login session remained valid: %v", err)
		}
	}
	if _, err := first.LoginAs(member.Login, "disable-race-password"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("disabled account login error=%v", err)
	}
}

func TestPasswordStoreWriteFailureLeavesOldCredentialsAndSessionsIntact(t *testing.T) {
	root := t.TempDir()
	service, token := openForTest(t, root)
	bootstrapForTest(t, service, token)
	service.bcryptCost = bcrypt.MinCost
	owner, err := service.FindByLogin("owner")
	if err != nil {
		t.Fatal(err)
	}
	session, err := service.Login(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	service.persistUsers = func(string, []byte) error { return errors.New("injected atomic store failure") }
	if err := service.ChangePassword(owner.ID, testPassword, "write-failed-password"); !errors.Is(err, ErrStorage) {
		t.Fatalf("password update error=%v", err)
	}
	if _, err := service.Login(testPassword); err != nil {
		t.Fatalf("old password was lost after failed write: %v", err)
	}
	if _, err := service.Login("write-failed-password"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("uncommitted password became active: %v", err)
	}
	if _, err := service.Authenticate(session.Token); err != nil {
		t.Fatalf("session was invalidated by failed store update: %v", err)
	}
}

func TestExistingUserAndSessionRecordsWithoutCredentialEpochRemainCompatible(t *testing.T) {
	root := t.TempDir()
	service, token := openForTest(t, root)
	bootstrapForTest(t, service, token)
	service.bcryptCost = bcrypt.MinCost
	session, err := service.Login(testPassword)
	if err != nil {
		t.Fatal(err)
	}
	usersPath := filepath.Join(root, securityDirectory, usersFilename)
	store, err := readUserStore(usersPath)
	if err != nil {
		t.Fatal(err)
	}
	store.Users[0].CredentialEpoch = 0
	data, err := encodeUserStore(store.Users)
	if err != nil {
		t.Fatal(err)
	}
	if err := atomicWrite(usersPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	legacySession, err := service.readSession(session.Token)
	if err != nil {
		t.Fatal(err)
	}
	legacySession.credentialEpoch = 0
	if err := service.writeSession(session.Token, legacySession); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenWithoutBootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Authenticate(session.Token); err != nil {
		t.Fatalf("legacy bound session was not preserved: %v", err)
	}
	user, err := reopened.GetUser(session.UserID)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.ChangePassword(user.ID, testPassword, "legacy-compatible-password"); err != nil {
		t.Fatalf("password change from legacy epoch: %v", err)
	}
	if _, err := reopened.Authenticate(session.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("legacy session survived credential epoch migration: %v", err)
	}
	updated, err := readUserStore(usersPath)
	if err != nil || updated.Users[0].CredentialEpoch != 1 {
		t.Fatalf("upgraded credential epoch=%d err=%v", updated.Users[0].CredentialEpoch, err)
	}
}

func TestAuthStoreProcessLockSerializesIndependentProcesses(t *testing.T) {
	if root := os.Getenv("IR_STAGE7_LOCK_ROOT"); root != "" {
		service, err := OpenWithoutBootstrap(root)
		if err != nil {
			t.Fatal(err)
		}
		if err := service.withAuthLock(func() error {
			return os.WriteFile(filepath.Join(root, "child-acquired-lock"), []byte("ok"), 0600)
		}); err != nil {
			t.Fatal(err)
		}
		return
	}

	root := t.TempDir()
	service, token := openForTest(t, root)
	bootstrapForTest(t, service, token)
	locked := make(chan struct{})
	release := make(chan struct{})
	lockErr := make(chan error, 1)
	go func() {
		lockErr <- service.withAuthLock(func() error {
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked
	command := exec.Command(os.Args[0], "-test.run=^TestAuthStoreProcessLockSerializesIndependentProcesses$")
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	command.Env = append(os.Environ(), "IR_STAGE7_LOCK_ROOT="+root)
	if err := command.Start(); err != nil {
		close(release)
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(root, "child-acquired-lock")); !errors.Is(err, os.ErrNotExist) {
		close(release)
		t.Fatalf("child entered shared auth lock before parent released it: %v", err)
	}
	close(release)
	if err := <-lockErr; err != nil {
		t.Fatalf("release parent auth lock: %v", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("child lock process failed: %v: %s", err, output.String())
	}
	if _, err := os.Stat(filepath.Join(root, "child-acquired-lock")); err != nil {
		t.Fatalf("child never acquired shared auth lock: %v", err)
	}
}

func TestUserDisableEnableOwnerProtectionAndSessionManagement(t *testing.T) {
	root := t.TempDir()
	service, token := openForTest(t, root)
	bootstrapForTest(t, service, token)
	service.bcryptCost = bcrypt.MinCost
	owner, err := service.FindByLogin("owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.SetUserEnabled(owner.ID, false); !errors.Is(err, ErrLastOwner) {
		t.Fatalf("last owner disable error = %v", err)
	}
	member, err := service.CreateUser("member-one", "member-password-one", RoleMember)
	if err != nil {
		t.Fatal(err)
	}
	memberSessionA, err := service.LoginAs(member.Login, "member-password-one")
	if err != nil {
		t.Fatal(err)
	}
	memberSessionB, err := service.LoginAs(member.Login, "member-password-one")
	if err != nil {
		t.Fatal(err)
	}
	items, err := service.ListSessions(member.ID, memberSessionA.Token)
	if err != nil || len(items) != 2 {
		t.Fatalf("session list=%+v err=%v", items, err)
	}
	currentCount := 0
	for _, item := range items {
		if item.Current {
			currentCount++
		}
		if item.ID == memberSessionA.Token || item.ID == memberSessionB.Token || item.ID == memberSessionA.CSRFToken {
			t.Fatal("session projection exposed a token or CSRF secret")
		}
	}
	if currentCount != 1 {
		t.Fatalf("current session count=%d", currentCount)
	}
	var otherID string
	for _, item := range items {
		if !item.Current {
			otherID = item.ID
		}
	}
	wasCurrent, err := service.RevokeSession(member.ID, otherID, memberSessionA.Token)
	if err != nil || wasCurrent {
		t.Fatalf("revoke other session current=%v err=%v", wasCurrent, err)
	}
	if _, err := service.Authenticate(memberSessionB.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revoked session still authenticated: %v", err)
	}
	memberSessionC, err := service.LoginAs(member.Login, "member-password-one")
	if err != nil {
		t.Fatal(err)
	}
	count, err := service.RevokeOtherSessions(member.ID, memberSessionA.Token)
	if err != nil || count != 1 {
		t.Fatalf("revoked other session count=%d err=%v", count, err)
	}
	if _, err := service.Authenticate(memberSessionA.Token); err != nil {
		t.Fatalf("current session was revoked: %v", err)
	}
	if _, err := service.Authenticate(memberSessionC.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("other session remained active: %v", err)
	}
	memberSessionD, err := service.LoginAs(member.Login, "member-password-one")
	if err != nil {
		t.Fatal(err)
	}
	dSessions, err := service.ListSessions(member.ID, memberSessionA.Token)
	if err != nil {
		t.Fatal(err)
	}
	var dSessionID string
	for _, item := range dSessions {
		if !item.Current {
			dSessionID = item.ID
		}
	}
	if dSessionID == "" {
		t.Fatal("new secondary session was not listed")
	}
	if err := service.Logout(memberSessionA.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := service.RevokeSession(member.ID, dSessionID, memberSessionA.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("stale caller was allowed to revoke a session: %v", err)
	}
	if _, err := service.Authenticate(memberSessionD.Token); err != nil {
		t.Fatalf("stale caller revoked a live secondary session: %v", err)
	}
	if _, err := service.SetUserEnabled(member.ID, false); err != nil {
		t.Fatalf("disable member: %v", err)
	}
	if _, err := service.Authenticate(memberSessionA.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("disabled user's session remained active: %v", err)
	}
	if _, err := service.Authenticate(memberSessionD.Token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("disabled user's second session remained active: %v", err)
	}
	if _, err := service.LoginAs(member.Login, "member-password-one"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("disabled user login error = %v", err)
	}
	if _, err := service.SetUserEnabled(member.ID, true); err != nil {
		t.Fatalf("enable member: %v", err)
	}
	if _, err := service.LoginAs(member.Login, "member-password-one"); err != nil {
		t.Fatalf("enabled user login: %v", err)
	}
}
