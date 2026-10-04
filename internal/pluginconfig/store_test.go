package pluginconfig

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

func TestConfigAndSecretStorageAreSeparateMaskedAndResourceScoped(t *testing.T) {
	root := t.TempDir()
	configs, secrets, err := NewTypedFileStores(root)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(configs, secrets)
	if err != nil {
		t.Fatal(err)
	}
	schema := adapterproto.Schema{Fields: []adapterproto.Field{{Key: "mode", Control: "select", Label: "Mode", Required: true, Options: []adapterproto.Option{{Value: "fast", Label: "Fast"}}}, {Key: "opaque_value", Control: "text", Label: "Value"}, {Key: "opaque_secret", Control: "secret", Label: "Secret"}}}
	resource := &adapterproto.ResourceRef{Type: "arbitrary.type", ID: "id/with/slashes", Parent: &adapterproto.ResourceRef{Type: "parent.kind", ID: "parent id"}}
	scope := Scope{PluginID: "plugin.example", Resource: resource}
	values := map[string]json.RawMessage{"mode": json.RawMessage(`"fast"`), "opaque_value": json.RawMessage(`"visible"`)}
	if err = service.Put(scope, schema, values, map[string]string{"opaque_secret": "sensitive-value"}); err != nil {
		t.Fatal(err)
	}
	masked, configured, err := service.Masked(scope)
	if err != nil {
		t.Fatal(err)
	}
	if configured["opaque_secret"] != true || configured["unknown"] {
		t.Fatalf("masked secret flags = %#v", configured)
	}
	if string(masked["mode"]) != `"fast"` {
		t.Fatalf("ordinary config not round-tripped: %s", masked["mode"])
	}
	encoded, err := json.Marshal(masked)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "sensitive-value") {
		t.Fatal("masked projection exposed secret plaintext")
	}
	other := Scope{PluginID: scope.PluginID, Resource: &adapterproto.ResourceRef{Type: "arbitrary.type", ID: "another"}}
	otherDoc, err := service.Get(other)
	if err != nil {
		t.Fatal(err)
	}
	if len(otherDoc.Values) != 0 || len(otherDoc.Secrets) != 0 {
		t.Fatalf("scoped values crossed resource boundary: %#v", otherDoc)
	}
	configFiles, err := os.ReadDir(filepath.Join(root, "plugin-config"))
	if err != nil {
		t.Fatal(err)
	}
	secretFiles, err := os.ReadDir(filepath.Join(root, "plugin-secrets"))
	if err != nil {
		t.Fatal(err)
	}
	if len(configFiles) != 1 || len(secretFiles) != 1 {
		t.Fatalf("config/secret files = %d/%d", len(configFiles), len(secretFiles))
	}
	configBytes, err := os.ReadFile(filepath.Join(root, "plugin-config", configFiles[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	secretBytes, err := os.ReadFile(filepath.Join(root, "plugin-secrets", secretFiles[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(configBytes), "sensitive-value") || !strings.Contains(string(secretBytes), "sensitive-value") {
		t.Fatal("secret was not kept in its separate store")
	}
	if info, err := os.Stat(filepath.Join(root, "plugin-secrets", secretFiles[0].Name())); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm()&0077 != 0 {
		t.Fatalf("secret file permissions = %o", info.Mode().Perm())
	}
}

func TestPutValidatesPluginDefinedSchemaAndOpaqueKeys(t *testing.T) {
	configs, secrets, err := NewTypedFileStores(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(configs, secrets)
	if err != nil {
		t.Fatal(err)
	}
	schema := adapterproto.Schema{Fields: []adapterproto.Field{{Key: "count", Control: "number", Label: "Count", Required: true, Constraints: &adapterproto.Constraints{Min: floatPtr(1), Max: floatPtr(4)}}, {Key: "password-ish", Control: "secret", Label: "Opaque secret"}}}
	scope := Scope{PluginID: "plugin", Resource: &adapterproto.ResourceRef{Type: "opaque.kind", ID: "123"}}
	if err = service.Put(scope, schema, map[string]json.RawMessage{"count": json.RawMessage(`5`)}, nil); err == nil {
		t.Fatal("out of range number accepted")
	}
	if err = service.Put(scope, schema, map[string]json.RawMessage{"count": json.RawMessage(`3`)}, map[string]string{"not-declared": "value"}); err == nil {
		t.Fatal("unknown plugin field accepted")
	}
	if err = service.Put(scope, schema, map[string]json.RawMessage{"count": json.RawMessage(`3`)}, map[string]string{"password-ish": "secret"}); err != nil {
		t.Fatal(err)
	}
}

func TestClearingReclassifiedSecretAlsoScrubsLegacyOrdinaryValue(t *testing.T) {
	configs, secrets, err := NewTypedFileStores(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(configs, secrets)
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{PluginID: "opaque-plugin"}
	legacySchema := adapterproto.Schema{Fields: []adapterproto.Field{{Key: "opaque_value", Control: "text", Label: "Value"}}}
	if err := service.Put(scope, legacySchema, map[string]json.RawMessage{"opaque_value": json.RawMessage(`"sensitive-old-value"`)}, nil); err != nil {
		t.Fatal(err)
	}
	secretSchema := adapterproto.Schema{Fields: []adapterproto.Field{{Key: "opaque_value", Control: "secret", Label: "Value", Persistence: &adapterproto.FieldPersistence{Mode: adapterproto.PersistenceOptional, Target: adapterproto.PersistenceTarget{Scope: adapterproto.PersistencePlugin}}}}}
	if err := service.PutPartialWithClears(scope, secretSchema, nil, nil, nil, []string{"opaque_value"}); err != nil {
		t.Fatal(err)
	}
	stored, err := service.Get(scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := stored.Values["opaque_value"]; exists {
		t.Fatal("legacy plaintext remained in ordinary configuration after explicit secret clear")
	}
	effective, err := service.Effective([]ScopeSchema{{Scope: scope, Schema: secretSchema}})
	if err != nil {
		t.Fatal(err)
	}
	if effective.EffectiveSecretConfigured["opaque_value"] || effective.StoredSecretConfigured["opaque_value"] {
		t.Fatalf("cleared legacy secret is still configured: %#v", effective.Snapshot())
	}
}

func floatPtr(v float64) *float64 { return &v }

type memorySecretStore struct {
	values map[string]map[string]string
}

var _ SecretStore = (*memorySecretStore)(nil)

func (s *memorySecretStore) Load(scope Scope) (map[string]string, error) {
	key, err := secretScopeKey(scope)
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	for name, value := range s.values[key] {
		values[name] = value
	}
	return values, nil
}

func (s *memorySecretStore) Save(scope Scope, values map[string]string) error {
	key, err := secretScopeKey(scope)
	if err != nil {
		return err
	}
	copy := map[string]string{}
	for name, value := range values {
		copy[name] = value
	}
	if s.values == nil {
		s.values = map[string]map[string]string{}
	}
	s.values[key] = copy
	return nil
}

type failNthSecretSave struct {
	inner  *memorySecretStore
	writes int
	failAt int
}

func (s *failNthSecretSave) Load(scope Scope) (map[string]string, error) {
	return s.inner.Load(scope)
}

func (s *failNthSecretSave) Save(scope Scope, values map[string]string) error {
	s.writes++
	if s.writes == s.failAt {
		return errors.New("injected secret backend failure")
	}
	return s.inner.Save(scope, values)
}

func secretScopeKey(scope Scope) (string, error) {
	data, err := json.Marshal(scope)
	return string(data), err
}

func TestSecretStoreInterfaceAcceptsNonFileBackendAndMasksValues(t *testing.T) {
	configs, _, err := NewTypedFileStores(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(configs, &memorySecretStore{})
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{PluginID: "memory-backed-plugin"}
	schema := adapterproto.Schema{Fields: []adapterproto.Field{{Key: "credential", Control: "secret", Label: "Opaque credential"}}}
	if err = service.Put(scope, schema, nil, map[string]string{"credential": "never-return-this"}); err != nil {
		t.Fatal(err)
	}
	values, configured, err := service.Masked(scope)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "never-return-this") || !configured["credential"] {
		t.Fatalf("non-file store projection leaked or lost secret state: values=%s configured=%#v", encoded, configured)
	}
}

func TestEffectiveHierarchyHonorsOpaqueScopesAndFieldInheritance(t *testing.T) {
	configs, secrets, err := NewTypedFileStores(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(configs, secrets)
	if err != nil {
		t.Fatal(err)
	}
	pluginSchema := adapterproto.Schema{Fields: []adapterproto.Field{
		{Key: "shared", Control: "text", Label: "Shared", Inherit: true},
		{Key: "local_only", Control: "text", Label: "Local"},
		{Key: "from_plugin", Control: "text", Label: "Plugin inherited", Inherit: true},
		{Key: "plugin_local", Control: "text", Label: "Plugin local"},
		{Key: "private", Control: "secret", Label: "Private"},
		{Key: "shared_secret", Control: "secret", Label: "Shared secret", Inherit: true},
	}}
	alphaSchema := adapterproto.Schema{Fields: []adapterproto.Field{
		{Key: "shared", Control: "text", Label: "Shared", Inherit: true},
		{Key: "local_only", Control: "text", Label: "Local"},
		{Key: "from_plugin", Control: "text", Label: "Plugin inherited", Inherit: true},
		{Key: "plugin_local", Control: "text", Label: "Plugin local"},
		{Key: "from_parent", Control: "text", Label: "Parent inherited", Inherit: true},
		{Key: "parent_local", Control: "text", Label: "Parent local"},
		{Key: "parent_secret", Control: "secret", Label: "Parent local secret"},
		{Key: "private", Control: "secret", Label: "Private"},
		{Key: "shared_secret", Control: "secret", Label: "Shared secret", Inherit: true},
	}}
	betaSchema := adapterproto.Schema{Fields: []adapterproto.Field{
		{Key: "shared", Control: "text", Label: "Shared"},
		{Key: "local_only", Control: "text", Label: "Local"},
		{Key: "from_plugin", Control: "text", Label: "Plugin inherited"},
		{Key: "plugin_local", Control: "text", Label: "Plugin local"},
		{Key: "from_parent", Control: "text", Label: "Parent inherited"},
		{Key: "parent_local", Control: "text", Label: "Parent local"},
		{Key: "parent_secret", Control: "secret", Label: "Parent local secret"},
		{Key: "private", Control: "secret", Label: "Private"},
		{Key: "shared_secret", Control: "secret", Label: "Shared secret", Inherit: true},
	}}
	root := Scope{PluginID: "opaque-plugin"}
	alpha := &adapterproto.ResourceRef{Type: "alpha", ID: "one"}
	beta := &adapterproto.ResourceRef{Type: "beta", ID: "two", Parent: alpha}
	if err = service.Put(root, pluginSchema, map[string]json.RawMessage{"shared": json.RawMessage(`"plugin"`), "local_only": json.RawMessage(`"plugin-local"`), "from_plugin": json.RawMessage(`"plugin-inherited"`), "plugin_local": json.RawMessage(`"plugin-local-only"`)}, map[string]string{"private": "plugin-secret", "shared_secret": "plugin-shared-secret"}); err != nil {
		t.Fatal(err)
	}
	if err = service.Put(Scope{PluginID: root.PluginID, Resource: alpha}, alphaSchema, map[string]json.RawMessage{"shared": json.RawMessage(`"alpha"`), "local_only": json.RawMessage(`"alpha-local"`), "from_parent": json.RawMessage(`"parent-inherited"`), "parent_local": json.RawMessage(`"parent-local-only"`)}, map[string]string{"private": "alpha-secret", "parent_secret": "ancestor-secret-only", "shared_secret": "alpha-secret-value"}); err != nil {
		t.Fatal(err)
	}
	if err = service.Put(Scope{PluginID: root.PluginID, Resource: beta}, betaSchema, map[string]json.RawMessage{"shared": json.RawMessage(`"beta"`), "local_only": json.RawMessage(`"beta-local"`)}, map[string]string{"private": "beta-secret"}); err != nil {
		t.Fatal(err)
	}
	unrelated := &adapterproto.ResourceRef{Type: "gamma", ID: "other"}
	if err = service.Put(Scope{PluginID: root.PluginID, Resource: unrelated}, betaSchema, map[string]json.RawMessage{"shared": json.RawMessage(`"unrelated"`)}, nil); err != nil {
		t.Fatal(err)
	}
	chain, err := ResourceScopes(root.PluginID, beta)
	if err != nil {
		t.Fatal(err)
	}
	effective, err := service.Effective([]ScopeSchema{{Scope: chain[0], Schema: pluginSchema}, {Scope: chain[1], Schema: alphaSchema}, {Scope: chain[2], Schema: betaSchema}})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(effective.EffectiveValues["shared"]); got != `"beta"` {
		t.Fatalf("child override = %s", got)
	}
	if got := effective.ValueSources["shared"]; got != ScopeIdentity(chain[2]) {
		t.Fatalf("child source = %q", got)
	}
	if got := string(effective.EffectiveValues["from_plugin"]); got != `"plugin-inherited"` {
		t.Fatalf("plugin inheritable value = %q", got)
	}
	if got := effective.ValueSources["from_plugin"]; got != ScopeIdentity(chain[0]) {
		t.Fatalf("plugin inherited source = %q", got)
	}
	if got := string(effective.EffectiveValues["from_parent"]); got != `"parent-inherited"` {
		t.Fatalf("parent inheritable value = %q", got)
	}
	if got := effective.ValueSources["from_parent"]; got != ScopeIdentity(chain[1]) {
		t.Fatalf("parent inherited source = %q", got)
	}
	if got := effective.EffectiveValues["local_only"]; string(got) != `"beta-local"` {
		t.Fatalf("child local value = %s", got)
	}
	for _, key := range []string{"plugin_local", "parent_local"} {
		if _, exists := effective.EffectiveValues[key]; exists {
			t.Fatalf("non-inheritable ancestor value %q reached child", key)
		}
	}
	if effective.EffectiveSecrets["private"] != "beta-secret" {
		t.Fatalf("local secret = %#v", effective.EffectiveSecrets)
	}
	if effective.EffectiveSecrets["shared_secret"] != "alpha-secret-value" {
		t.Fatalf("inherited secret = %#v", effective.EffectiveSecrets)
	}
	if _, exists := effective.EffectiveSecrets["parent_secret"]; exists {
		t.Fatal("non-inheritable ancestor secret reached child")
	}
	if effective.EffectiveSecretConfigured["private"] != true || effective.SecretSources["private"] != ScopeIdentity(chain[2]) {
		t.Fatal("secret flags/provenance missing")
	}
	if _, ok := effective.EffectiveValues["not-present"]; ok {
		t.Fatal("unrelated scope value was mixed in")
	}
	if len(chain) != 3 || chain[1].Resource.Type != "alpha" || chain[2].Resource.Type != "beta" {
		t.Fatalf("scope order = %#v", chain)
	}
	if err = service.PutPartialWithClears(chain[2], betaSchema, nil, map[string]string{"private": ""}, []string{"shared"}, nil); err != nil {
		t.Fatal(err)
	}
	cleared, err := service.Effective([]ScopeSchema{{Scope: chain[0], Schema: pluginSchema}, {Scope: chain[1], Schema: alphaSchema}, {Scope: chain[2], Schema: betaSchema}})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(cleared.EffectiveValues["shared"]); got != `"alpha"` {
		t.Fatalf("cleared override did not reveal parent: %s", got)
	}
	if cleared.EffectiveSecrets["private"] != "beta-secret" {
		t.Fatal("empty secret update cleared an existing secret")
	}
}

func TestPutBatchRollsBackEarlierScopeWhenLaterSecretWriteFails(t *testing.T) {
	configs, _, err := NewTypedFileStores(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	secrets := &failNthSecretSave{inner: &memorySecretStore{}, failAt: 2}
	service, err := NewService(configs, secrets)
	if err != nil {
		t.Fatal(err)
	}
	pluginScope := Scope{PluginID: "opaque"}
	resourceScope := Scope{PluginID: "opaque", Resource: &adapterproto.ResourceRef{Type: "beta", ID: "child"}}
	pluginSchema := adapterproto.Schema{Fields: []adapterproto.Field{{Key: "ordinary", Control: "text", Label: "Ordinary"}}}
	resourceSchema := adapterproto.Schema{Fields: []adapterproto.Field{{Key: "secret", Control: "secret", Label: "Secret"}}}
	if err = service.Put(pluginScope, pluginSchema, map[string]json.RawMessage{"ordinary": json.RawMessage(`"before"`)}, nil); err != nil {
		t.Fatal(err)
	}
	secrets.failAt = 0
	if err = service.Put(resourceScope, resourceSchema, nil, map[string]string{"secret": "before-secret"}); err != nil {
		t.Fatal(err)
	}
	secrets.writes = 0
	secrets.failAt = 2
	err = service.PutBatch([]ScopeUpdate{
		{Scope: pluginScope, Schema: pluginSchema, Values: map[string]json.RawMessage{"ordinary": json.RawMessage(`"after"`)}},
		{Scope: resourceScope, Schema: resourceSchema, Secrets: map[string]string{"secret": "after-secret"}},
	})
	if err == nil {
		t.Fatal("failing batch unexpectedly succeeded")
	}
	plugin, err := service.Get(pluginScope)
	if err != nil || string(plugin.Values["ordinary"]) != `"before"` {
		t.Fatalf("plugin scope was not rolled back: %#v, %v", plugin, err)
	}
	resource, err := service.Get(resourceScope)
	if err != nil || resource.Secrets["secret"] != "before-secret" {
		t.Fatalf("resource scope was not restored: %#v, %v", resource, err)
	}
}

func TestResourceScopeDepthLimitRemainsEnforced(t *testing.T) {
	var ref *adapterproto.ResourceRef
	for i := 0; i < 65; i++ {
		ref = &adapterproto.ResourceRef{Type: "opaque", ID: string(rune('a' + i%20)), Parent: ref}
	}
	if _, err := ResourceScopes("plugin", ref); err == nil {
		t.Fatal("over-depth scope chain accepted")
	}
}
