package authn

import (
	"path/filepath"
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
