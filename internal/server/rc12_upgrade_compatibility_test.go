package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/nodeidentity"
	"github.com/integrated-recorder/core/internal/pluginconfig"
	"github.com/integrated-recorder/core/internal/runtimehost/generation"
	"github.com/integrated-recorder/core/internal/runtimehost/installation"
	"github.com/integrated-recorder/core/internal/runtimehost/storagecatalog"
	"github.com/integrated-recorder/core/internal/storageproto"
	"github.com/integrated-recorder/core/internal/systemsettings"
)

// TestV02IdentityInitializationPreservesRC12LocalAccountsSessionsAndArchive
// pins the migration boundary: opening the separate v0.2 identity stores must
// neither reinterpret RC12 local accounts as Global Identity nor rewrite the
// canonical recording tree. Existing local sessions remain usable after the
// new stores have been initialized.
func TestV02IdentityInitializationPreservesRC12LocalAccountsSessionsAndArchive(t *testing.T) {
	root := t.TempDir()
	localAuth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := localAuth.Bootstrap("rc12-owner", "rc12-owner-password"); err != nil {
		t.Fatal(err)
	}
	ownerBefore, err := localAuth.FindByLogin("rc12-owner")
	if err != nil {
		t.Fatal(err)
	}
	memberBefore, err := localAuth.CreateUser("rc12-viewer", "rc12-viewer-password", authn.RoleViewer)
	if err != nil {
		t.Fatal(err)
	}
	ownerSession, err := localAuth.LoginAs(ownerBefore.Login, "rc12-owner-password")
	if err != nil {
		t.Fatal(err)
	}

	// Model durable RC12 management and runtime state alongside local auth.
	// These stores are intentionally populated through their public APIs so
	// the test exercises valid on-disk formats rather than hand-written blobs.
	settings, err := systemsettings.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	theme := "dark"
	if _, err := settings.Update(systemsettings.Patch{UITheme: &theme}); err != nil {
		t.Fatal(err)
	}
	configs, secrets, err := pluginconfig.NewTypedFileStores(root)
	if err != nil {
		t.Fatal(err)
	}
	pluginSettings, err := pluginconfig.NewService(configs, secrets)
	if err != nil {
		t.Fatal(err)
	}
	pluginSchema := adapterproto.Schema{Fields: []adapterproto.Field{
		{Key: "quality", Control: "text", Label: "Quality"},
		{Key: "credential", Control: "secret", Label: "Credential"},
	}}
	pluginScope := pluginconfig.Scope{PluginID: "chzzk.source"}
	if err := pluginSettings.Put(pluginScope, pluginSchema, map[string]json.RawMessage{"quality": json.RawMessage(`"source"`)}, map[string]string{"credential": "fixture-secret"}); err != nil {
		t.Fatal(err)
	}

	registryPath := filepath.Join(root, "runtime", "state", "generations.json")
	registry, err := generation.Open(registryPath)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Date(2025, 8, 1, 2, 3, 4, 0, time.UTC)
	const activeGenerationID = "11111111111111111111111111111111"
	const recordingID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const adapterSetID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const storageSetID = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	const storageInstanceID = "si_0123456789abcdef0123456789abcdef"
	runtimeGeneration := generation.Generation{
		ID: activeGenerationID, Version: "0.1.0-rc.12", Commit: strings.Repeat("1", 40),
		AdapterSetID: adapterSetID, StorageProviderSetID: storageSetID, StorageInstanceID: storageInstanceID,
		InstalledAt: startedAt, State: generation.StateStaging, ControlProtocol: 1, EngineProtocol: 1,
		ArchiveReadCompatibility: generation.CompatibilityRange{Minimum: generation.CurrentArchiveFormatVersion, Maximum: generation.CurrentArchiveFormatVersion},
		ArchiveWriteFormat:       generation.CurrentArchiveFormatVersion,
	}
	if err := registry.Stage(runtimeGeneration); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkVerified(activeGenerationID); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkReady(activeGenerationID); err != nil {
		t.Fatal(err)
	}
	if err := registry.Activate(activeGenerationID); err != nil {
		t.Fatal(err)
	}
	if err := registry.FinalizeActivation(activeGenerationID); err != nil {
		t.Fatal(err)
	}
	if err := registry.PinRecording(generation.Lease{RecordingID: recordingID, EngineGeneration: activeGenerationID, WorkerInstance: "dddddddddddddddddddddddddddddddd", StartedAt: startedAt}); err != nil {
		t.Fatal(err)
	}

	// Preserve an actually probed Storage Provider artifact, immutable config
	// set with a private credential, selected storage instance, and generation
	// pins. This catches a migration that leaves generation pins intact but
	// loses the provider bytes or catalog objects those pins refer to.
	providerBinary := filepath.Join(t.TempDir(), "fixture-storage-provider")
	providerBuild := exec.Command("go", "build", "-trimpath", "-o", providerBinary, "../runtimehost/storagecatalog/testdata/secretprovider")
	if output, err := providerBuild.CombinedOutput(); err != nil {
		t.Fatalf("build storage provider compatibility fixture: %v (%s)", err, output)
	}
	providerBytes, err := os.ReadFile(providerBinary)
	if err != nil {
		t.Fatal(err)
	}
	providerDigest := sha256.Sum256(providerBytes)
	providerCatalogRoot := filepath.Join(root, "runtime", "storage-providers")
	providerCatalog, err := storagecatalog.Open(providerCatalogRoot)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { makeTreeWritableForCleanup(providerCatalogRoot) })
	providerArtifact, err := providerCatalog.Import(context.Background(), providerBinary, storagecatalog.Expected{
		ID: "secret-storage", Version: "1.0.0", ProtocolVersion: storageproto.Version,
		SHA256: hex.EncodeToString(providerDigest[:]), Size: int64(len(providerBytes)),
	})
	if err != nil {
		t.Fatalf("import Storage Provider fixture: %v", err)
	}
	providerConfig := storagecatalog.SetConfig{Secrets: map[string]string{"access_token": "rc12-storage-fixture-secret"}}
	providerSet, err := providerCatalog.CreateSet(providerArtifact.Digest, providerConfig)
	if err != nil {
		t.Fatalf("create pinned Storage Provider set: %v", err)
	}
	if err := providerCatalog.Install(providerArtifact); err != nil {
		t.Fatal(err)
	}
	if err := providerCatalog.SelectDesiredSet(providerArtifact.ID, providerSet.ID); err != nil {
		t.Fatal(err)
	}
	providerInstance, err := providerCatalog.CreateStorageInstance("RC12 archive storage", providerArtifact.ID, providerSet.ID)
	if err != nil {
		t.Fatalf("create pinned Storage Provider instance: %v", err)
	}

	archiveDir := filepath.Join(root, "recordings", "4d6f3c8208754a8cbff5064a171c2490")
	copyFixtureTree(t, filepath.Join("..", "acquire", "testdata", "rc12-sharded-archive"), archiveDir)

	// Capture the exact legacy local account/session bytes and the real RC12
	// archive fixture before v0.2 creates any independent identity state.
	protectedPaths := []string{
		filepath.Join(root, "security", "users.json"),
		filepath.Join(root, "security", "sessions"),
		filepath.Join(root, "management", "system-settings.json"),
		filepath.Join(root, "plugin-config"),
		filepath.Join(root, "plugin-secrets"),
		registryPath,
		providerCatalogRoot,
		archiveDir,
	}
	before := captureProtectedTree(t, root, protectedPaths...)

	identity, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, initialized, err := identity.Snapshot(); err != nil || initialized {
		t.Fatalf("opening v0.2 stores implicitly initialized a Global Identity: initialized=%v err=%v", initialized, err)
	}
	node, err := nodeidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	createdNode, err := node.Create()
	if err != nil {
		t.Fatal(err)
	}

	// Opening the stores again models a Runtime Host restart. It must not
	// promote local accounts to a Global Identity or rotate the installation's
	// durable Node Key.
	reopenedIdentity, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, initialized, err := reopenedIdentity.Snapshot(); err != nil || initialized {
		t.Fatalf("reopening v0.2 stores implicitly initialized a Global Identity: initialized=%v err=%v", initialized, err)
	}
	reopenedNode, err := nodeidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	nodeAfterRestart, err := reopenedNode.Info()
	if err != nil || nodeAfterRestart.NodeID != createdNode.NodeID || nodeAfterRestart.PublicKey != createdNode.PublicKey {
		t.Fatalf("Node Key changed across v0.2 store restart: before=%+v after=%+v err=%v", createdNode, nodeAfterRestart, err)
	}

	after := captureProtectedTree(t, root, protectedPaths...)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("v0.2 identity initialization changed RC12 account/session/archive bytes:\nbefore=%v\nafter=%v", before, after)
	}

	reopenedAuth, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	ownerAfter, err := reopenedAuth.FindByLogin("rc12-owner")
	if err != nil {
		t.Fatal(err)
	}
	memberAfter, err := reopenedAuth.FindByLogin("rc12-viewer")
	if err != nil {
		t.Fatal(err)
	}
	if ownerAfter.ID != ownerBefore.ID || memberAfter.ID != memberBefore.ID || ownerAfter.Roles[0] != authn.RoleOwner || memberAfter.Roles[0] != authn.RoleViewer {
		t.Fatalf("RC12 local account identities/roles changed: owner=%+v member=%+v", ownerAfter, memberAfter)
	}
	if _, err := reopenedAuth.Authenticate(ownerSession.Token); err != nil {
		t.Fatalf("RC12 local session did not survive v0.2 identity initialization: %v", err)
	}
	reopenedSettings, err := systemsettings.Open(root)
	if err != nil || reopenedSettings.Current().UI.Theme != theme {
		t.Fatalf("RC12 system settings changed across v0.2 initialization: settings=%+v err=%v", reopenedSettings, err)
	}
	reopenedConfigs, reopenedSecrets, err := pluginconfig.NewTypedFileStores(root)
	if err != nil {
		t.Fatal(err)
	}
	reopenedPluginSettings, err := pluginconfig.NewService(reopenedConfigs, reopenedSecrets)
	if err != nil {
		t.Fatal(err)
	}
	pluginDocument, err := reopenedPluginSettings.Get(pluginScope)
	if err != nil || string(pluginDocument.Values["quality"]) != `"source"` || pluginDocument.Secrets["credential"] != "fixture-secret" {
		t.Fatalf("RC12 plugin settings changed across v0.2 initialization: document=%+v err=%v", pluginDocument, err)
	}
	reopenedRegistry, err := generation.Open(registryPath)
	if err != nil {
		t.Fatal(err)
	}
	registryState := reopenedRegistry.Snapshot()
	if registryState.ActiveGenerationID != activeGenerationID || registryState.Generations[activeGenerationID].Version != "0.1.0-rc.12" || registryState.Generations[activeGenerationID].AdapterSetID != adapterSetID || registryState.Generations[activeGenerationID].StorageProviderSetID != storageSetID || registryState.Generations[activeGenerationID].StorageInstanceID != storageInstanceID {
		t.Fatalf("RC12 generation or provider pins changed across v0.2 initialization: %+v", registryState)
	}
	if lease, ok := registryState.Leases[recordingID]; !ok || lease.EngineGeneration != activeGenerationID {
		t.Fatalf("RC12 active recording lease was not preserved: lease=%+v exists=%v", lease, ok)
	}
	reopenedProviderCatalog, err := storagecatalog.Open(providerCatalogRoot)
	if err != nil {
		t.Fatal(err)
	}
	installedProviders, err := reopenedProviderCatalog.Installed()
	if err != nil || len(installedProviders) != 1 || installedProviders[0] != providerArtifact {
		t.Fatalf("RC12 installed Storage Provider artifact changed: installed=%+v err=%v", installedProviders, err)
	}
	selectedProviderSet, err := reopenedProviderCatalog.DesiredSet(providerArtifact.ID)
	if err != nil || selectedProviderSet.ID != providerSet.ID || selectedProviderSet.Artifact.Digest != providerArtifact.Digest || !reflect.DeepEqual(selectedProviderSet.Config.Secrets, providerConfig.Secrets) {
		t.Fatalf("RC12 selected Storage Provider set changed: set=%+v err=%v", selectedProviderSet, err)
	}
	instances, err := reopenedProviderCatalog.ListStorageInstances()
	instancePreserved := false
	for _, instance := range instances {
		if instance.ID == providerInstance.ID && instance == providerInstance && instance.DesiredSetID == providerSet.ID {
			instancePreserved = true
		}
	}
	if err != nil || !instancePreserved {
		t.Fatalf("RC12 Storage Provider instance changed: instances=%+v err=%v", instances, err)
	}
}

func TestV02IdentityInitializationDoesNotReopenBootstrapForCorruptRC12Auth(t *testing.T) {
	root := t.TempDir()
	// macOS may expose the temporary directory through a symlinked /var path;
	// installation.Reconcile deliberately rejects symlinked ancestors.
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	root = resolvedRoot
	securityDir := filepath.Join(root, "security")
	if err := os.MkdirAll(securityDir, 0700); err != nil {
		t.Fatal(err)
	}
	corruptUsers := []byte(`{"version":2,"users":[{"user_id":"owner","login":"owner"`)
	usersPath := filepath.Join(securityDir, "users.json")
	if err := os.WriteFile(usersPath, corruptUsers, 0600); err != nil {
		t.Fatal(err)
	}

	identity, err := globalidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, initialized, err := identity.Snapshot(); err != nil || initialized {
		t.Fatalf("corrupt local auth state caused implicit Global Identity creation: initialized=%v err=%v", initialized, err)
	}
	node, err := nodeidentity.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.Create(); err != nil {
		t.Fatal(err)
	}

	// Runtime opens authentication in recovery mode. Corruption is not treated
	// as a fresh install, and the bootstrap API remains unavailable.
	auth, err := authn.OpenWithoutBootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Bootstrap("new-owner", "new-owner-password"); err != authn.ErrBootstrapUnavailable {
		t.Fatalf("corrupt RC12 auth permitted a fresh Owner bootstrap: %v", err)
	}
	if configured, inspectErr := authn.InspectAdministrator(root); inspectErr == nil || configured {
		t.Fatalf("corrupt administrator state was mistaken for a fresh install: configured=%v err=%v", configured, inspectErr)
	}
	installationState, err := installation.Reconcile(root, installation.AdminUnknown, false)
	if err != nil {
		t.Fatalf("initialize installation recovery state for corrupt RC12 data: %v", err)
	}
	snapshot := installationState.Snapshot()
	if snapshot.State != installation.StateRecoveryRequired || snapshot.DiagnosticCode != "administrator_state_unavailable" {
		t.Fatalf("corrupt RC12 state did not enter recovery-required: snapshot=%+v", snapshot)
	}
	got, err := os.ReadFile(usersPath)
	if err != nil || !reflect.DeepEqual(got, corruptUsers) {
		t.Fatalf("corrupt local auth state was modified: bytes=%q err=%v", got, err)
	}
}

func copyFixtureTree(t *testing.T, source, destination string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return os.ErrInvalid
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0600)
	})
	if err != nil {
		t.Fatalf("copy isolated RC12 archive fixture: %v", err)
	}
}

func captureProtectedTree(t *testing.T, root string, paths ...string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for _, protected := range paths {
		err := filepath.WalkDir(protected, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return os.ErrInvalid
			}
			if entry.IsDir() {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			digest := sha256.Sum256(data)
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			out[relative] = hex.EncodeToString(digest[:])
			return nil
		})
		if err != nil {
			t.Fatalf("inventory protected test fixture: %v", err)
		}
	}
	return out
}

func makeTreeWritableForCleanup(root string) {
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if entry.IsDir() {
			_ = os.Chmod(path, 0700)
		} else {
			_ = os.Chmod(path, 0600)
		}
		return nil
	})
}
