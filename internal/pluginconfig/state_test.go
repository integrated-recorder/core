package pluginconfig

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

func TestAdapterStateAndSecretStatePersistSeparatelyAcrossServiceRestart(t *testing.T) {
	root := t.TempDir()
	values, secrets, err := NewStateFileStores(root)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewStateService(values, secrets)
	if err != nil {
		t.Fatal(err)
	}
	parent := Scope{PluginID: "opaque.adapter", Resource: &adapterproto.ResourceRef{Type: "alpha", ID: "parent"}}
	child := Scope{PluginID: "opaque.adapter", Resource: &adapterproto.ResourceRef{Type: "beta", ID: "child", Parent: &adapterproto.ResourceRef{Type: "alpha", ID: "parent"}}}
	if err = service.Apply([]StateMutation{{Scope: Scope{PluginID: "opaque.adapter"}, Secrets: map[string]string{"session": "private-state-sentinel"}}, {Scope: parent, Values: map[string]json.RawMessage{"generation": json.RawMessage(`1`)}}, {Scope: Scope{PluginID: "another.adapter"}, Secrets: map[string]string{"session": "isolated-state"}}}); err != nil {
		t.Fatal(err)
	}
	if err = service.Apply([]StateMutation{{Scope: child, Secrets: map[string]string{"session": "child-only"}}}); err != nil {
		t.Fatal(err)
	}
	valueFiles, err := os.ReadDir(filepath.Join(root, "adapter-state"))
	if err != nil {
		t.Fatal(err)
	}
	secretFiles, err := os.ReadDir(filepath.Join(root, "adapter-state-secrets"))
	if err != nil {
		t.Fatal(err)
	}
	if len(valueFiles) != 1 || len(secretFiles) != 3 {
		t.Fatalf("state file counts = %d/%d", len(valueFiles), len(secretFiles))
	}
	for _, item := range secretFiles {
		info, statErr := item.Info()
		if statErr != nil || info.Mode().Perm()&0077 != 0 {
			t.Fatalf("secret state permissions: %v %v", info, statErr)
		}
	}
	for _, dir := range []string{"adapter-state", "adapter-state-secrets"} {
		info, statErr := os.Stat(filepath.Join(root, dir))
		if statErr != nil || info.Mode().Perm()&0077 != 0 {
			t.Fatalf("state directory permissions: %v %v", info, statErr)
		}
	}
	// Recreate the service over the same backends to model a Core process restart.
	values, secrets, err = NewStateFileStores(root)
	if err != nil {
		t.Fatal(err)
	}
	service, err = NewStateService(values, secrets)
	if err != nil {
		t.Fatal(err)
	}
	chain, err := service.Chain([]Scope{{PluginID: "opaque.adapter"}, parent, child})
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) != 3 || chain[0].Secrets["session"] != "private-state-sentinel" || string(chain[1].Values["generation"]) != "1" || chain[2].Secrets["session"] != "child-only" {
		t.Fatalf("state chain did not reload: %#v", chain)
	}
	other, err := service.Get(Scope{PluginID: "another.adapter"})
	if err != nil || other.Secrets["session"] != "isolated-state" {
		t.Fatalf("plugin state scopes collided: %#v, %v", other, err)
	}
}

type rollbackConfigStore struct{ values map[string]json.RawMessage }

func (s *rollbackConfigStore) Load(Scope) (map[string]json.RawMessage, error) {
	return cloneRaw(s.values), nil
}
func (s *rollbackConfigStore) Save(_ Scope, values map[string]json.RawMessage) error {
	s.values = cloneRaw(values)
	return nil
}

type failOnceSecretStore struct {
	values map[string]string
	fail   bool
}

func (s *failOnceSecretStore) Load(Scope) (map[string]string, error) {
	return cloneSecrets(s.values), nil
}
func (s *failOnceSecretStore) Save(_ Scope, values map[string]string) error {
	if s.fail {
		s.fail = false
		return errors.New("injected backend failure")
	}
	s.values = cloneSecrets(values)
	return nil
}

func TestPutPartialRollsBackOrdinaryValuesWhenSecretStoreFails(t *testing.T) {
	config := &rollbackConfigStore{values: map[string]json.RawMessage{"old": json.RawMessage(`"before"`)}}
	secret := &failOnceSecretStore{values: map[string]string{"old_secret": "before-secret"}, fail: true}
	service, err := NewService(config, secret)
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{PluginID: "rollback.adapter"}
	schema := adapterproto.Schema{Fields: []adapterproto.Field{{Key: "old", Control: "text", Label: "Old"}, {Key: "new_secret", Control: "secret", Label: "Secret"}}}
	err = service.PutPartial(scope, schema, map[string]json.RawMessage{"old": json.RawMessage(`"after"`)}, map[string]string{"new_secret": "secret"}, nil)
	if err == nil {
		t.Fatal("injected secret save failure was hidden")
	}
	if string(config.values["old"]) != `"before"` || len(config.values) != 1 || secret.values["old_secret"] != "before-secret" {
		t.Fatalf("stores were left partially committed: config=%#v secrets=%#v", config.values, secret.values)
	}
}
