package installation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/watch"
)

func testRoot(t *testing.T) string {
	t.Helper()
	base := os.TempDir()
	if resolved, resolveErr := filepath.EvalSymlinks(base); resolveErr == nil {
		base = resolved
	}
	root, err := os.MkdirTemp(base, fmt.Sprintf("ir-installation-%d-", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return root
}

func TestFreshInstallAndAtomicReadyTransition(t *testing.T) {
	root := testRoot(t)
	store, err := Reconcile(root, AdminMissing, false)
	if err != nil {
		t.Fatal(err)
	}
	initial := store.Snapshot()
	if initial.State != StateUninitialized || !installationIDRE.MatchString(initial.InstallationID) {
		t.Fatalf("fresh state=%+v", initial)
	}
	if err := os.MkdirAll(filepath.Join(root, "security"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(false); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("begin without administrator = %v", err)
	}
	if _, err := store.Begin(true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(); err != nil {
		t.Fatal(err)
	}
	ready := store.Snapshot()
	if ready.State != StateReady || ready.ReadyAt.IsZero() || ready.InstallationID != initial.InstallationID {
		t.Fatalf("ready state=%+v", ready)
	}
	if _, err := store.Begin(true); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("ready -> setup accepted: %v", err)
	}
	info, err := os.Lstat(filepath.Join(root, "runtime"))
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("runtime directory mode info=%v err=%v", info, err)
	}
	info, err = os.Lstat(filepath.Join(root, "runtime", Filename))
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatalf("installation state mode info=%v err=%v", info, err)
	}
}

func TestReconcileLegacyAndResumeAfterAdministratorBootstrap(t *testing.T) {
	legacy := testRoot(t)
	if err := os.MkdirAll(filepath.Join(legacy, "security"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacy, "security", "admin.json"), []byte(`{"password_hash":"legacy"}`), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := Reconcile(legacy, AdminConfigured, false)
	if err != nil || store.Snapshot().State != StateReady {
		t.Fatalf("legacy migration state=%+v err=%v", store.Snapshot(), err)
	}

	fresh := testRoot(t)
	store, err = Reconcile(fresh, AdminMissing, false)
	if err != nil || store.Snapshot().State != StateUninitialized {
		t.Fatalf("initial state=%+v err=%v", store.Snapshot(), err)
	}
	reloaded, err := Reconcile(fresh, AdminConfigured, false)
	if err != nil || reloaded.Snapshot().State != StateSetupInProgress {
		t.Fatalf("post-bootstrap resume state=%+v err=%v", reloaded.Snapshot(), err)
	}
}

func TestValidLegacyAdministratorMigratesReadyWithoutChangingProductData(t *testing.T) {
	root := testRoot(t)
	auth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	setupCode, err := authn.ReadSetupCode(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap(setupCode, "legacy-install-migration-password"); err != nil {
		t.Fatal(err)
	}
	installationPath := filepath.Join(root, "runtime", Filename)
	if _, err := os.Lstat(installationPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fixture unexpectedly has Runtime Host installation state: %v", err)
	}

	// A pre-lifecycle installation may already contain arbitrary canonical and
	// product-management data. Migration may add only the Host-owned record.
	watchID := strings.Repeat("a", 32)
	createdAt := time.Now().UTC().Format(time.RFC3339Nano)
	watchDefinition := []byte(fmt.Sprintf(`{"id":%q,"adapter_id":"owncast","input":{"instance_url":"https://owncast.example"},"enabled":true,"preview_mode":"disabled","check_interval_seconds":10,"created_at":%q,"updated_at":%q}`, watchID, createdAt, createdAt))
	existing := map[string][]byte{
		filepath.Join(root, "recordings", "legacy-recording", "recording.json"):                []byte(`{"id":"legacy"}`),
		filepath.Join(root, "recordings", "legacy-recording", "tracks", "main", "00000001.ts"): []byte("canonical-segment"),
		filepath.Join(root, "management", "settings", "system-settings.json"):                  []byte(`{"theme":"dark"}`),
		filepath.Join(root, "management", "watches", "definitions", watchID+".json"):           watchDefinition,
	}
	for path, data := range existing {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	configured, err := authn.InspectAdministrator(root)
	if err != nil || !configured {
		t.Fatalf("administrator inspection configured=%v err=%v", configured, err)
	}
	adminState := AdminMissing
	if configured {
		adminState = AdminConfigured
	}
	store, err := Reconcile(root, adminState, false)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := store.Snapshot()
	if snapshot.State != StateReady || snapshot.InstallationID == "" || snapshot.ReadyAt.IsZero() {
		t.Fatalf("legacy administrator was not migrated to ready: %+v", snapshot)
	}
	for path, want := range existing {
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("legacy data changed at %s: got=%q err=%v", filepath.Base(path), got, err)
		}
	}
	if _, err := authn.ReadSetupCode(root); !errors.Is(err, authn.ErrBootstrapUnavailable) {
		t.Fatalf("legacy administrator migration left a claimable setup code: %v", err)
	}
	watchStore, err := watch.Open(root)
	if err != nil {
		t.Fatalf("legacy Watch definitions did not remain readable after migration: %v", err)
	}
	if got := watchStore.List(); len(got) != 1 || got[0].ID != watchID {
		t.Fatalf("legacy Watch definition was not preserved: %+v", got)
	}
}

func TestRecoveryRequiredAndCorruptStateIsPreserved(t *testing.T) {
	root := testRoot(t)
	store, err := Reconcile(root, AdminMissing, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Reconcile(root, AdminMissing, false)
	if err != nil || recovered.Snapshot().State != StateRecoveryRequired || recovered.Snapshot().DiagnosticCode != "administrator_state_missing" {
		t.Fatalf("ready+missing administrator state=%+v err=%v", recovered.Snapshot(), err)
	}

	corruptRoot := testRoot(t)
	if err := os.MkdirAll(filepath.Join(corruptRoot, "runtime"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(corruptRoot, "runtime", Filename)
	original := []byte(`{"schema_version":99,"secret":"do not overwrite"}`)
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	corrupt, err := Reconcile(corruptRoot, AdminMissing, false)
	if err != nil || corrupt.Snapshot().State != StateRecoveryRequired {
		t.Fatalf("corrupt state=%+v err=%v", corrupt.Snapshot(), err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(original) {
		t.Fatalf("corrupt state was rewritten: %q err=%v", got, err)
	}
}

func TestSymlinkAndNonRegularStateFailClosed(t *testing.T) {
	for _, kind := range []string{"symlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			root := testRoot(t)
			runtimeDir := filepath.Join(root, "runtime")
			if err := os.Mkdir(runtimeDir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(runtimeDir, Filename)
			if kind == "symlink" {
				if err := os.Symlink(filepath.Join(root, "outside"), path); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			store, err := Reconcile(root, AdminMissing, false)
			if err != nil || store.Snapshot().State != StateRecoveryRequired {
				t.Fatalf("state=%+v err=%v", store.Snapshot(), err)
			}
		})
	}
}

func TestInstallationStateWithLoosePermissionsFailsClosed(t *testing.T) {
	root := testRoot(t)
	store, err := Reconcile(root, AdminMissing, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(store.path, 0644); err != nil {
		t.Fatal(err)
	}
	reloaded, err := Reconcile(root, AdminMissing, false)
	if err != nil || reloaded.Snapshot().State != StateRecoveryRequired {
		t.Fatalf("loose state permissions state=%+v err=%v", reloaded.Snapshot(), err)
	}
	info, err := os.Lstat(store.path)
	if err != nil || info.Mode().Perm() != 0644 {
		t.Fatalf("invalid state was unexpectedly rewritten: info=%v err=%v", info, err)
	}
}

func TestStoreRevalidatesStateBeforeReadinessAndTransitions(t *testing.T) {
	root := testRoot(t)
	store, err := Reconcile(root, AdminMissing, false)
	if err != nil {
		t.Fatal(err)
	}
	path := store.path
	if err := os.WriteFile(path, []byte(`{malformed`), 0600); err != nil {
		t.Fatal(err)
	}
	if store.Ready() || store.Snapshot().State != StateRecoveryRequired {
		t.Fatal("corruption after startup did not fail readiness closed")
	}
	if _, err := store.Begin(true); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("transition after corruption error=%v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != `{malformed` {
		t.Fatalf("corrupt state was overwritten: %q err=%v", data, err)
	}
}

func TestConcurrentCompletionHasSingleTransition(t *testing.T) {
	root := testRoot(t)
	store, err := Reconcile(root, AdminMissing, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(true); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < cap(errs); i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := store.Complete()
			errs <- err
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("completion returned error: %v", err)
		}
	}
	if got := store.Snapshot().State; got != StateReady {
		t.Fatalf("final state=%q", got)
	}
}

func TestWriteFailureDoesNotPublishTransition(t *testing.T) {
	store, err := Reconcile(testRoot(t), AdminMissing, false)
	if err != nil {
		t.Fatal(err)
	}
	store.write = func(string, Record) error { return errors.New("injected fsync failure") }
	if _, err := store.Begin(true); err == nil {
		t.Fatal("injected durable-write failure was ignored")
	}
	if got := store.Snapshot().State; got != StateUninitialized {
		t.Fatalf("failed transition published state %q", got)
	}
}

func TestUnknownSchemaAndMalformedJSONAreRecoveryRequired(t *testing.T) {
	for _, data := range []string{`{"schema_version":1,"installation_id":"` + strings.Repeat("a", 32) + `","state":"uninitialized","created_at":"` + time.Now().UTC().Format(time.RFC3339Nano) + `","updated_at":"` + time.Now().UTC().Format(time.RFC3339Nano) + `","future":true}`, `{not-json`} {
		root := testRoot(t)
		if err := os.Mkdir(filepath.Join(root, "runtime"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "runtime", Filename), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		store, err := Reconcile(root, AdminMissing, false)
		if err != nil || store.Snapshot().State != StateRecoveryRequired {
			t.Fatalf("data=%q state=%+v err=%v", data, store.Snapshot(), err)
		}
	}
}

func TestAuthDisabledInitializesReady(t *testing.T) {
	store, err := Reconcile(testRoot(t), AdminMissing, true)
	if err != nil || store.Snapshot().State != StateReady {
		t.Fatalf("state=%+v err=%v", store.Snapshot(), err)
	}
}

func TestRecordEncodingIsStrictAndBounded(t *testing.T) {
	root := testRoot(t)
	store, err := Reconcile(root, AdminMissing, false)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(store.Snapshot().Record)
	if err != nil || len(data) > maxRecordSize {
		t.Fatalf("record encoding bytes=%d err=%v", len(data), err)
	}
}
