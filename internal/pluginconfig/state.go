package pluginconfig

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

// StateDoc stores adapter-owned opaque state at one scope. Secrets are kept in
// a separate backend and are returned only to trusted Core call paths.
type StateDoc struct {
	Values  map[string]json.RawMessage
	Secrets map[string]string
}

type StateMutation struct {
	Scope        Scope
	Values       map[string]json.RawMessage
	Secrets      map[string]string
	ClearValues  []string
	ClearSecrets []string
}

type AdapterStateStore interface {
	Load(Scope) (map[string]json.RawMessage, error)
	Save(Scope, map[string]json.RawMessage) error
}

type AdapterSecretStateStore interface {
	Load(Scope) (map[string]string, error)
	Save(Scope, map[string]string) error
}

type StateService struct {
	values  AdapterStateStore
	secrets AdapterSecretStateStore
	mu      sync.Mutex
}

type preparedStateMutation struct {
	scope         Scope
	before, after StateDoc
}

func NewStateService(values AdapterStateStore, secrets AdapterSecretStateStore) (*StateService, error) {
	if values == nil || secrets == nil {
		return nil, fmt.Errorf("adapter state stores are required")
	}
	return &StateService{values: values, secrets: secrets}, nil
}

func (s *StateService) Get(scope Scope) (StateDoc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getUnlocked(scope)
}

func (s *StateService) getUnlocked(scope Scope) (StateDoc, error) {
	if err := validateScope(scope); err != nil {
		return StateDoc{}, err
	}
	values, err := s.values.Load(scope)
	if err != nil {
		return StateDoc{}, err
	}
	secrets, err := s.secrets.Load(scope)
	if err != nil {
		return StateDoc{}, err
	}
	if values == nil {
		values = map[string]json.RawMessage{}
	}
	if secrets == nil {
		secrets = map[string]string{}
	}
	if err := validateStateValues(values); err != nil {
		return StateDoc{}, fmt.Errorf("invalid adapter state values")
	}
	if err := validateStateSecrets(secrets); err != nil {
		return StateDoc{}, fmt.Errorf("invalid adapter state secrets")
	}
	return StateDoc{Values: cloneRaw(values), Secrets: cloneSecrets(secrets)}, nil
}

// Chain loads state in increasing specificity order. Caller-provided scopes
// must already have been validated against the adapter descriptor.
func (s *StateService) Chain(scopes []Scope) ([]StateDoc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	docs := make([]StateDoc, 0, len(scopes))
	for _, scope := range scopes {
		doc, err := s.getUnlocked(scope)
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

// Apply merges adapter-owned state mutations. The caller validates that every
// target belongs to the active resource chain. On a failed backend write it
// attempts to restore every touched scope to its previous snapshot.
func (s *StateService) Apply(mutations []StateMutation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(mutations) == 0 {
		return nil
	}
	items := make([]preparedStateMutation, 0, len(mutations))
	index := map[string]int{}
	for _, mutation := range mutations {
		if err := validateScope(mutation.Scope); err != nil {
			return fmt.Errorf("invalid adapter state scope")
		}
		if err := validateStateValues(mutation.Values); err != nil {
			return err
		}
		if err := validateStateSecrets(mutation.Secrets); err != nil {
			return err
		}
		for _, key := range append(append([]string(nil), mutation.ClearValues...), mutation.ClearSecrets...) {
			if !adapterproto.IsValidIdentifier(key) {
				return fmt.Errorf("invalid adapter state clear key")
			}
		}
		identity, err := json.Marshal(mutation.Scope)
		if err != nil {
			return fmt.Errorf("encode adapter state scope")
		}
		key := string(identity)
		at, exists := index[key]
		if !exists {
			before, err := s.getUnlocked(mutation.Scope)
			if err != nil {
				return fmt.Errorf("load adapter state: %w", err)
			}
			at = len(items)
			index[key] = at
			items = append(items, preparedStateMutation{scope: mutation.Scope, before: before, after: StateDoc{Values: cloneRaw(before.Values), Secrets: cloneSecrets(before.Secrets)}})
		}
		after := &items[at].after
		for k, v := range mutation.Values {
			after.Values[k] = append(json.RawMessage(nil), v...)
		}
		for k, v := range mutation.Secrets {
			after.Secrets[k] = v
		}
		for _, k := range mutation.ClearValues {
			delete(after.Values, k)
		}
		for _, k := range mutation.ClearSecrets {
			delete(after.Secrets, k)
		}
	}
	for i, item := range items {
		if err := s.values.Save(item.scope, item.after.Values); err != nil {
			return s.rollback(items[:i+1], fmt.Errorf("save adapter state values: %w", err))
		}
		if err := s.secrets.Save(item.scope, item.after.Secrets); err != nil {
			return s.rollback(items[:i+1], fmt.Errorf("save adapter state secrets: %w", err))
		}
	}
	return nil
}

func (s *StateService) rollback(items []preparedStateMutation, cause error) error {
	var failures []string
	for i := len(items) - 1; i >= 0; i-- {
		if err := s.values.Save(items[i].scope, cloneRaw(items[i].before.Values)); err != nil {
			failures = append(failures, "values rollback failed")
		}
		if err := s.secrets.Save(items[i].scope, cloneSecrets(items[i].before.Secrets)); err != nil {
			failures = append(failures, "secret state rollback failed")
		}
	}
	if len(failures) != 0 {
		return fmt.Errorf("%w; %s", cause, strings.Join(failures, ", "))
	}
	return cause
}

func validateStateValues(values map[string]json.RawMessage) error {
	for key, value := range values {
		if !adapterproto.IsValidIdentifier(key) || !json.Valid(value) {
			return fmt.Errorf("invalid adapter state value")
		}
	}
	return nil
}

func validateStateSecrets(values map[string]string) error {
	for key := range values {
		if !adapterproto.IsValidIdentifier(key) {
			return fmt.Errorf("invalid adapter state secret")
		}
	}
	return nil
}

type StateFileStore struct{ fileStore }
type StateSecretFileStore struct{ fileStore }

func (f StateFileStore) Load(scope Scope) (map[string]json.RawMessage, error) {
	data, err := f.fileStore.loadBytes(scope)
	if err != nil {
		return nil, err
	}
	values := map[string]json.RawMessage{}
	err = json.Unmarshal(data, &values)
	return values, err
}
func (f StateFileStore) Save(scope Scope, values map[string]json.RawMessage) error {
	return f.fileStore.save(scope, values)
}
func (f StateSecretFileStore) Load(scope Scope) (map[string]string, error) {
	data, err := f.fileStore.loadBytes(scope)
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	err = json.Unmarshal(data, &values)
	return values, err
}
func (f StateSecretFileStore) Save(scope Scope, values map[string]string) error {
	return f.fileStore.save(scope, values)
}

func NewStateFileStores(root string) (AdapterStateStore, AdapterSecretStateStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, nil, fmt.Errorf("adapter state directory is empty")
	}
	values := StateFileStore{fileStore{root: filepath.Join(root, "adapter-state")}}
	secrets := StateSecretFileStore{fileStore{root: filepath.Join(root, "adapter-state-secrets")}}
	for _, dir := range []string{values.root, secrets.root} {
		if err := ensurePrivateDirectory(dir); err != nil {
			return nil, nil, err
		}
	}
	return values, secrets, nil
}

func ensurePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	return os.Chmod(path, 0700)
}
