package authn

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func TestV1UserStoreMigrationIsAtomicIdempotentAndDoesNotExpandMember(t *testing.T) {
	root := t.TempDir()
	security := filepath.Join(root, securityDirectory)
	if err := os.MkdirAll(filepath.Join(security, sessionDirectory), 0700); err != nil {
		t.Fatal(err)
	}
	ownerHash, err := bcrypt.GenerateFromPassword([]byte("legacy-owner-password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	memberHash, err := bcrypt.GenerateFromPassword([]byte("legacy-member-password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	legacy := legacyPersistedUserStore{Version: 1, Users: []legacyUserRecord{
		{ID: "usr-0123456789abcdef0123456789abcdef", Login: "owner", Role: RoleOwner, Status: UserStatusActive, CreatedAt: created, PasswordHash: string(ownerHash)},
		{ID: "usr-1123456789abcdef0123456789abcdef", Login: "member-a", Role: "member", Status: UserStatusActive, CreatedAt: created, PasswordHash: string(memberHash)},
	}}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(security, usersFilename)
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	service, err := OpenWithoutBootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	migrated, err := readUserStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if migrated.Version != 2 || len(migrated.Users) != 2 || migrated.Users[0].ID != legacy.Users[0].ID || migrated.Users[1].ID != legacy.Users[1].ID {
		t.Fatalf("migration changed schema/user identity: %+v", migrated)
	}
	if migrated.Users[0].Login != "owner" || migrated.Users[0].Roles[0] != RoleOwner || string(migrated.Users[0].PasswordHash) != string(ownerHash) {
		t.Fatalf("owner migration changed login, role, or hash: %+v", migrated.Users[0])
	}
	member := migrated.Users[1]
	if member.Login != "member-a" || len(member.Roles) != 1 || member.Roles[0] != RoleOperator || string(member.PasswordHash) != string(memberHash) {
		t.Fatalf("member migration changed login/hash or failed safe mapping: %+v", member)
	}
	permissions := permissionsForRoles(member.Roles, migrated.CustomRoles)
	want := []Permission{PermissionRecordingAnnotate, PermissionRecordingCreate, PermissionRecordingExport, PermissionRecordingIntegrity, PermissionRecordingPreview, PermissionRecordingRead, PermissionRecordingStop, PermissionRecordingWorkflow, PermissionWatchCheck, PermissionAccountProfileSelf, PermissionAccountPasswordSelf, PermissionAccountSessionsSelf, PermissionUserPreferences}
	if !samePermissions(permissions, want) {
		t.Fatalf("Operator migration authority=%v, want exact old recording.read/control surface %v", permissions, want)
	}
	if _, err := service.LoginAs("member-a", "legacy-member-password"); err != nil {
		t.Fatalf("migrated member password no longer authenticates: %v", err)
	}
	firstBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWithoutBootstrap(root); err != nil {
		t.Fatal(err)
	}
	secondBytes, err := os.ReadFile(path)
	if err != nil || string(firstBytes) != string(secondBytes) {
		t.Fatalf("reopening changed migrated state: err=%v", err)
	}
}

func TestUserStoreFailsClosedWithoutAnActiveOwner(t *testing.T) {
	root := t.TempDir()
	security := filepath.Join(root, securityDirectory)
	if err := os.MkdirAll(security, 0700); err != nil {
		t.Fatal(err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("viewer-without-owner-password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	viewer, err := newUserRecord("viewer-only", []string{RoleViewer}, string(hash), time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	data, err := encodeUserStore(persistedUserStore{Version: userStoreVersion, Users: []userRecord{viewer}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(security, usersFilename)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readUserStore(path); !errors.Is(err, ErrCorruptStore) {
		t.Fatalf("store without an active Owner error=%v, want fail-closed corrupt-store error", err)
	}
}

func samePermissions(actual, expected []Permission) bool {
	if len(actual) != len(expected) {
		return false
	}
	actualSet := make(map[Permission]struct{}, len(actual))
	for _, permission := range actual {
		actualSet[permission] = struct{}{}
	}
	for _, permission := range expected {
		if _, ok := actualSet[permission]; !ok {
			return false
		}
	}
	return true
}

func TestConcurrentCaseInsensitiveUserCreationAndLoginRename(t *testing.T) {
	root := t.TempDir()
	first, token := openForTest(t, root)
	bootstrapForTest(t, first, token)
	first.bcryptCost = bcrypt.MinCost
	second, err := OpenWithoutBootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	second.bcryptCost = bcrypt.MinCost
	services := []*Service{first, second}
	start := make(chan struct{})
	var wg sync.WaitGroup
	createErrors := make([]error, 2)
	for index, login := range []string{"DuplicateLogin", "duplicatelogin"} {
		wg.Add(1)
		go func(index int, login string) {
			defer wg.Done()
			<-start
			_, createErrors[index] = services[index].CreateUser(login, "duplicate-user-password", RoleViewer)
		}(index, login)
	}
	close(start)
	wg.Wait()
	successes, duplicates := 0, 0
	for _, err := range createErrors {
		if err == nil {
			successes++
		} else if errors.Is(err, ErrDuplicateLogin) {
			duplicates++
		} else {
			t.Fatalf("unexpected create error: %v", err)
		}
	}
	if successes != 1 || duplicates != 1 {
		t.Fatalf("case-insensitive create race successes=%d duplicates=%d errors=%v", successes, duplicates, createErrors)
	}

	owner, err := first.FindByLogin("owner")
	if err != nil {
		t.Fatal(err)
	}
	secondOwner, err := first.CreateUserWithRolesAs(owner.ID, "rename-racer", "rename-racer-password", []string{RoleOwner})
	if err != nil {
		t.Fatal(err)
	}
	start = make(chan struct{})
	renameErrors := make([]error, 2)
	for index, id := range []string{owner.ID, secondOwner.ID} {
		wg.Add(1)
		go func(index int, id string, service *Service) {
			defer wg.Done()
			<-start
			password := testPassword
			if id == secondOwner.ID {
				password = "rename-racer-password"
			}
			_, renameErrors[index] = service.RenameLogin(id, password, "shared-target")
		}(index, id, services[index])
	}
	close(start)
	wg.Wait()
	successes, duplicates = 0, 0
	for _, err := range renameErrors {
		if err == nil {
			successes++
		} else if errors.Is(err, ErrDuplicateLogin) {
			duplicates++
		} else {
			t.Fatalf("unexpected rename error: %v", err)
		}
	}
	if successes != 1 || duplicates != 1 {
		t.Fatalf("rename race successes=%d duplicates=%d errors=%v", successes, duplicates, renameErrors)
	}
}

func TestAdministratorCannotAssignProtectedOwnerRole(t *testing.T) {
	root := t.TempDir()
	service, token := openForTest(t, root)
	bootstrapForTest(t, service, token)
	service.bcryptCost = bcrypt.MinCost
	owner, err := service.FindByLogin("owner")
	if err != nil {
		t.Fatal(err)
	}
	admin, err := service.CreateUserWithRolesAs(owner.ID, "administrator-a", "administrator-password", []string{RoleAdministrator})
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.CreateUserWithRolesAs(admin.ID, "should-not-be-owner", "another-valid-password", []string{RoleOwner})
	if !errors.Is(err, ErrOwnerAssignment) || created.ID != "" {
		t.Fatalf("Administrator Owner assignment result=%+v err=%v", created, err)
	}
	if _, err := service.SetUserRoles(admin.ID, owner.ID, []string{RoleOperator}); !errors.Is(err, ErrOwnerAssignment) {
		t.Fatalf("Administrator Owner removal error=%v", err)
	}
}

func TestConcurrentFinalOwnerDisableAndDeleteKeepOneOwner(t *testing.T) {
	root := t.TempDir()
	first, token := openForTest(t, root)
	bootstrapForTest(t, first, token)
	first.bcryptCost = bcrypt.MinCost
	ownerA, err := first.FindByLogin("owner")
	if err != nil {
		t.Fatal(err)
	}
	ownerB, err := first.CreateUserWithRolesAs(ownerA.ID, "second-owner", "second-owner-password", []string{RoleOwner})
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenWithoutBootstrap(root)
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make([]error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, results[0] = first.SetUserEnabledAs(ownerA.ID, ownerB.ID, false)
	}()
	go func() {
		defer wg.Done()
		<-start
		_, results[1] = second.DeleteUser(ownerB.ID, ownerA.ID)
	}()
	close(start)
	wg.Wait()

	for _, err := range results {
		if err != nil && !errors.Is(err, ErrUnauthenticated) && !errors.Is(err, ErrUserNotFound) {
			t.Fatalf("unexpected concurrent Owner removal error: %v", err)
		}
	}
	users, err := first.ListUsers()
	if err != nil {
		t.Fatal(err)
	}
	if countActiveOwners(users) != 1 {
		t.Fatalf("concurrent disable/delete left %d active Owners; users=%+v", countActiveOwners(users), users)
	}
}

func TestConcurrentCustomRoleDeleteAndAssignmentNeverLeavesDanglingRole(t *testing.T) {
	root := t.TempDir()
	first, token := openForTest(t, root)
	bootstrapForTest(t, first, token)
	first.bcryptCost = bcrypt.MinCost
	owner, err := first.FindByLogin("owner")
	if err != nil {
		t.Fatal(err)
	}
	user, err := first.CreateUserWithRolesAs(owner.ID, "role-race-user", "role-race-password", []string{RoleViewer})
	if err != nil {
		t.Fatal(err)
	}
	role, err := first.CreateCustomRole("Racing role", []Permission{PermissionSystemDiagnostics})
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenWithoutBootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var deleteErr, assignErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		deleteErr = first.DeleteCustomRole(role.ID)
	}()
	go func() {
		defer wg.Done()
		<-start
		_, assignErr = second.SetUserRoles(owner.ID, user.ID, []string{RoleViewer, role.ID})
	}()
	close(start)
	wg.Wait()

	store, err := readUserStore(filepath.Join(root, securityDirectory, usersFilename))
	if err != nil {
		t.Fatal(err)
	}
	roleExists := false
	for _, candidate := range store.CustomRoles {
		roleExists = roleExists || candidate.ID == role.ID
	}
	assigned := false
	for _, candidate := range store.Users {
		if candidate.ID == user.ID {
			assigned = hasRole(candidate.Roles, role.ID)
		}
	}
	if roleExists != assigned {
		t.Fatalf("role delete/assignment left inconsistent state: role_exists=%v assigned=%v delete_err=%v assign_err=%v", roleExists, assigned, deleteErr, assignErr)
	}
	if roleExists {
		if !errors.Is(deleteErr, ErrRoleInUse) || assignErr != nil {
			t.Fatalf("assignment won but operation errors disagree: delete=%v assign=%v", deleteErr, assignErr)
		}
	} else if deleteErr != nil || !errors.Is(assignErr, ErrInvalidRole) {
		t.Fatalf("delete won but operation errors disagree: delete=%v assign=%v", deleteErr, assignErr)
	}
}

func TestConcurrentCustomRoleEditsAreFreshForAssignedSession(t *testing.T) {
	root := t.TempDir()
	first, token := openForTest(t, root)
	bootstrapForTest(t, first, token)
	first.bcryptCost = bcrypt.MinCost
	owner, err := first.FindByLogin("owner")
	if err != nil {
		t.Fatal(err)
	}
	permissionsA := []Permission{PermissionRecordingRead, PermissionSystemDiagnostics}
	permissionsB := []Permission{PermissionRecordingRead, PermissionStorageRead}
	role, err := first.CreateCustomRole("Concurrent edit role", permissionsA)
	if err != nil {
		t.Fatal(err)
	}
	user, err := first.CreateUserWithRolesAs(owner.ID, "role-edit-user", "role-edit-password", []string{role.ID})
	if err != nil {
		t.Fatal(err)
	}
	session, err := first.LoginAs(user.Login, "role-edit-password")
	if err != nil {
		t.Fatal(err)
	}
	second, err := OpenWithoutBootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	wantA := permissionsForRoles([]string{role.ID}, []Role{{ID: role.ID, Permissions: permissionsA}})
	wantB := permissionsForRoles([]string{role.ID}, []Role{{ID: role.ID, Permissions: permissionsB}})
	start := make(chan struct{})
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for index := 0; index < 32; index++ {
			permissions := permissionsA
			if index%2 == 1 {
				permissions = permissionsB
			}
			if _, err := second.UpdateCustomRole(role.ID, role.Name, permissions); err != nil {
				errCh <- err
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for index := 0; index < 64; index++ {
			current, err := first.Authenticate(session.Token)
			if err != nil {
				errCh <- err
				return
			}
			if !samePermissions(current.Permissions, wantA) && !samePermissions(current.Permissions, wantB) {
				errCh <- fmt.Errorf("session observed partial custom-role permissions: %v", current.Permissions)
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func countActiveOwners(users []User) int {
	count := 0
	for _, user := range users {
		if user.Status == UserStatusActive && hasRole(user.Roles, RoleOwner) {
			count++
		}
	}
	return count
}
