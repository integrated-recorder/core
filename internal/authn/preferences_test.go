package authn

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestUserPreferenceStorageKeyIsUserScopedAndSeparateFromSystemSettings(t *testing.T) {
	root := t.TempDir()
	firstID := "usr-11111111111111111111111111111111"
	secondID := "usr-22222222222222222222222222222222"

	firstKey := UserPreferenceKey{UserID: firstID}
	secondKey := UserPreferenceKey{UserID: secondID}
	first, err := userPreferencesPath(root, firstKey)
	if err != nil {
		t.Fatal(err)
	}
	second, err := userPreferencesPath(root, secondKey)
	if err != nil {
		t.Fatal(err)
	}
	wantFirst := filepath.Join(root, "management", userPreferencesDirectory, firstID+".json")
	wantSecond := filepath.Join(root, "management", userPreferencesDirectory, secondID+".json")
	if first != wantFirst || second != wantSecond || first == second {
		t.Fatalf("preference paths first=%q second=%q; want stable user-keyed paths", first, second)
	}
	if first == filepath.Join(root, "management", "system-settings.json") || filepath.Dir(first) == filepath.Join(root, "security") {
		t.Fatalf("user preferences share system/authentication storage: %q", first)
	}
	if _, err := userPreferencesPath(root, UserPreferenceKey{UserID: "not-a-user-id"}); err == nil {
		t.Fatal("invalid user ID accepted as a preference key")
	}
}

func TestUserPreferencesDefaultIsolationAndPersistence(t *testing.T) {
	root := t.TempDir()
	service := openPreferencesTestService(t, root)
	var err error
	first, err := service.CreateUser("prefs-first", "prefs-first-password", RoleOwner)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.CreateUser("prefs-second", "prefs-second-password", RoleOwner)
	if err != nil {
		t.Fatal(err)
	}

	defaults := DefaultUserPreferences()
	got, err := service.GetUserPreferences(first.ID)
	if err != nil || got != defaults {
		t.Fatalf("missing preferences=%+v err=%v, want defaults %+v", got, err, defaults)
	}
	firstPrefs := UserPreferences{Locale: "ko-KR", Theme: "dark", Timezone: "Asia/Seoul"}
	secondPrefs := UserPreferences{Locale: "en-US", Theme: "light", Timezone: "America/Los_Angeles"}
	if err := service.SetUserPreferences(first.ID, firstPrefs); err != nil {
		t.Fatal(err)
	}
	if err := service.SetUserPreferences(second.ID, secondPrefs); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	gotFirst, err := reopened.GetUserPreferences(first.ID)
	if err != nil || gotFirst != firstPrefs {
		t.Fatalf("reopened first preferences=%+v err=%v", gotFirst, err)
	}
	gotSecond, err := reopened.GetUserPreferences(second.ID)
	if err != nil || gotSecond != secondPrefs {
		t.Fatalf("reopened second preferences=%+v err=%v", gotSecond, err)
	}
	path, err := userPreferencesPath(root, UserPreferenceKey{UserID: first.ID})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("preference file mode=%#o, want 0600", info.Mode().Perm())
	}
}

func TestUserPreferencesValidationAndForwardCompatibleFields(t *testing.T) {
	root := t.TempDir()
	service := openPreferencesTestService(t, root)
	var err error
	user, err := service.CreateUser("prefs-validation", "prefs-validation-password", RoleOwner)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []UserPreferences{
		{Locale: "fr-FR", Theme: "dark", Timezone: "system"},
		{Locale: "en-US", Theme: "sepia", Timezone: "system"},
		{Locale: "ko-KR", Theme: "light", Timezone: "Mars/Olympus"},
		{Locale: "system", Theme: "system", Timezone: " Asia/Seoul"},
		{Locale: "system", Theme: "system", Timezone: "Local"},
	} {
		if err := service.SetUserPreferences(user.ID, invalid); !errors.Is(err, ErrInvalidPreferences) {
			t.Errorf("invalid preferences %+v err=%v, want ErrInvalidPreferences", invalid, err)
		}
	}
	for _, timezone := range []string{"UTC", "Asia/Seoul"} {
		if err := ValidateUserPreferences(UserPreferences{Locale: "system", Theme: "system", Timezone: timezone}); err != nil {
			t.Errorf("valid IANA timezone %q rejected: %v", timezone, err)
		}
	}

	if err := service.SetUserPreferences(user.ID, UserPreferences{Locale: "en-US", Theme: "dark", Timezone: "system"}); err != nil {
		t.Fatal(err)
	}
	path, err := userPreferencesPath(root, UserPreferenceKey{UserID: user.ID})
	if err != nil {
		t.Fatal(err)
	}
	var persisted map[string]any
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	persisted["future_hint"] = "ignored by older reader"
	data, err = json.Marshal(persisted)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := service.GetUserPreferences(user.ID)
	want := UserPreferences{Locale: "en-US", Theme: "dark", Timezone: "system"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("forward-compatible read=%+v err=%v, want %+v", got, err, want)
	}
}

func TestCorruptedPreferencesFallBackWithoutBlockingAuthOrUpdate(t *testing.T) {
	root := t.TempDir()
	service := openPreferencesTestService(t, root)
	var err error
	user, err := service.CreateUser("prefs-corrupt", "prefs-corrupt-password", RoleOwner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.LoginAs(user.Login, "prefs-corrupt-password"); err != nil {
		t.Fatalf("login before preference corruption: %v", err)
	}
	path, err := userPreferencesPath(root, UserPreferenceKey{UserID: user.ID})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{broken"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := service.GetUserPreferences(user.ID)
	if err != nil || got != DefaultUserPreferences() {
		t.Fatalf("corrupt preferences=%+v err=%v, want default fallback", got, err)
	}
	if _, err := service.LoginAs(user.Login, "prefs-corrupt-password"); err != nil {
		t.Fatalf("login after preference corruption: %v", err)
	}
	repaired := UserPreferences{Locale: "ko-KR", Theme: "light", Timezone: "system"}
	if err := service.SetUserPreferences(user.ID, repaired); err != nil {
		t.Fatalf("replace corrupt preferences: %v", err)
	}
	got, err = service.GetUserPreferences(user.ID)
	if err != nil || got != repaired {
		t.Fatalf("repaired preferences=%+v err=%v", got, err)
	}
}

func TestUnknownPreferencesVersionCannotBeOverwritten(t *testing.T) {
	root := t.TempDir()
	service := openPreferencesTestService(t, root)
	var err error
	user, err := service.CreateUser("prefs-future", "prefs-future-password", RoleOwner)
	if err != nil {
		t.Fatal(err)
	}
	path, err := userPreferencesPath(root, UserPreferenceKey{UserID: user.ID})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"version":2,"locale":"en-US","theme":"dark","timezone":"system"}`), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := service.GetUserPreferences(user.ID)
	if err != nil || got != DefaultUserPreferences() {
		t.Fatalf("future version read=%+v err=%v, want defaults", got, err)
	}
	if err := service.SetUserPreferences(user.ID, UserPreferences{Locale: "en-US", Theme: "dark", Timezone: "system"}); !errors.Is(err, ErrUnsupportedPreferencesVersion) {
		t.Fatalf("future version overwrite err=%v, want ErrUnsupportedPreferencesVersion", err)
	}
}

func openPreferencesTestService(t *testing.T, root string) *Service {
	t.Helper()
	service, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	token, err := ReadSetupCode(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Bootstrap(token, "preferences-test-bootstrap-password"); err != nil {
		t.Fatal(err)
	}
	return service
}
