package watch

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

const (
	maxStoreBytes          = 256 << 10
	maxDefinitionFileBytes = 32 << 10
	maxRuntimeFileBytes    = 8 << 10
	maxEventFileBytes      = 256 << 10
	maxRelationFileBytes   = 4 << 10
)

var watchIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

type storeDocument struct {
	Definitions map[string]Definition        `json:"definitions"`
	Runtimes    map[string]Runtime           `json:"runtimes"`
	Events      map[string][]Event           `json:"events"`
	Relations   map[string]RecordingRelation `json:"relations"`
}

// Store persists bounded Watch projections as separate per-Watch files and
// stores input secrets in dedicated private files. A runtime transition only
// rewrites that Watch's runtime file, never a global state document.
type Store struct {
	mu             sync.Mutex
	root           string
	definitionRoot string
	runtimeRoot    string
	eventRoot      string
	secretRoot     string
	relationRoot   string
	secretBytes    int64
	secretSet      map[string]map[string]bool
	document       storeDocument
}

func Open(root string) (*Store, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("watch data directory is empty")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	base := filepath.Join(abs, "management", "watches")
	definitionRoot := filepath.Join(base, "definitions")
	runtimeRoot := filepath.Join(base, "runtime")
	eventRoot := filepath.Join(base, "events")
	secretRoot := filepath.Join(base, "secrets")
	relationRoot := filepath.Join(base, "relations")
	for _, dir := range []string{filepath.Join(abs, "management"), base, definitionRoot, runtimeRoot, eventRoot, secretRoot, relationRoot} {
		if err := privateDir(dir); err != nil {
			return nil, err
		}
	}
	s := &Store{root: base, definitionRoot: definitionRoot, runtimeRoot: runtimeRoot, eventRoot: eventRoot, secretRoot: secretRoot, relationRoot: relationRoot, secretSet: map[string]map[string]bool{}, document: emptyDocument()}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func emptyDocument() storeDocument {
	return storeDocument{Definitions: map[string]Definition{}, Runtimes: map[string]Runtime{}, Events: map[string][]Event{}, Relations: map[string]RecordingRelation{}}
}

func (s *Store) load() error {
	definitions, err := jsonFiles(s.definitionRoot, MaxWatches)
	if err != nil {
		return err
	}
	for _, item := range definitions {
		id := strings.TrimSuffix(item.Name(), ".json")
		var def Definition
		if err := readJSON(filepath.Join(s.definitionRoot, item.Name()), maxDefinitionFileBytes, &def); err != nil || def.ID != id || def.AdapterID == "" || len(def.Input) == 0 || len(def.Input) > MaxInputBytes || def.CheckIntervalSeconds < MinIntervalSeconds || def.CheckIntervalSeconds > MaxIntervalSeconds || (def.PreviewMode != "disabled" && def.PreviewMode != "segment") {
			return errors.New("persisted watch definition is invalid")
		}
		s.document.Definitions[id] = def
	}
	if len(s.document.Definitions) > MaxWatches {
		return errors.New("watch count exceeds configured bound")
	}
	runtimes, err := jsonFiles(s.runtimeRoot, MaxWatches)
	if err != nil {
		return err
	}
	for _, item := range runtimes {
		id := strings.TrimSuffix(item.Name(), ".json")
		var runtime Runtime
		if _, ok := s.document.Definitions[id]; !ok {
			if err := removeRegular(filepath.Join(s.runtimeRoot, item.Name())); err != nil {
				return err
			}
			continue
		}
		if err := readJSON(filepath.Join(s.runtimeRoot, item.Name()), maxRuntimeFileBytes, &runtime); err != nil || !validRuntime(runtime) {
			return errors.New("persisted watch runtime is invalid")
		}
		s.document.Runtimes[id] = runtime
	}
	for id, def := range s.document.Definitions {
		if _, ok := s.document.Runtimes[id]; !ok {
			state := StateChecking
			if !def.Enabled {
				state = StateDisabled
			}
			s.document.Runtimes[id] = Runtime{State: state}
		}
		secrets, err := readSecrets(s.secretPath(id))
		if err != nil {
			return errors.New("persisted watch secrets are invalid")
		}
		s.secretBytes += secretSize(secrets)
		s.secretSet[id] = configuredFlags(secrets)
		if s.secretBytes > MaxAggregateSecretBytes {
			return errors.New("aggregate watch secrets exceed configured bound")
		}
	}
	events, err := jsonFiles(s.eventRoot, MaxWatches)
	if err != nil {
		return err
	}
	for _, item := range events {
		id := strings.TrimSuffix(item.Name(), ".json")
		if _, ok := s.document.Definitions[id]; !ok {
			if err := removeRegular(filepath.Join(s.eventRoot, item.Name())); err != nil {
				return err
			}
			continue
		}
		var list []Event
		if err := readJSON(filepath.Join(s.eventRoot, item.Name()), maxEventFileBytes, &list); err != nil || len(list) > MaxEventsPerWatch {
			return errors.New("persisted watch events are invalid")
		}
		for _, event := range list {
			if !validEvent(event) {
				return errors.New("persisted watch event is invalid")
			}
		}
		s.document.Events[id] = list
	}
	if totalEvents(s.document.Events) > MaxEventsTotal {
		previous := cloneEventMap(s.document.Events)
		trimEvents(s.document.Events, MaxEventsTotal)
		if err := s.persistEventChanges(previous); err != nil {
			return err
		}
	}
	if err := s.loadRelations(); err != nil {
		return err
	}
	return s.cleanupOrphanFiles()
}

func (s *Store) cleanupOrphanFiles() error {
	entries, err := jsonFiles(s.secretRoot, MaxWatches)
	if err != nil {
		return err
	}
	for _, item := range entries {
		id := strings.TrimSuffix(item.Name(), ".json")
		if _, ok := s.document.Definitions[id]; !ok {
			if err := removeRegular(filepath.Join(s.secretRoot, item.Name())); err != nil {
				return err
			}
		}
	}
	return syncDir(s.secretRoot)
}

func (s *Store) loadRelations() error {
	entries, err := jsonFiles(s.relationRoot, MaxRelations+1)
	if err != nil {
		return err
	}
	for _, item := range entries {
		id := strings.TrimSuffix(item.Name(), ".json")
		var relation RecordingRelation
		path := filepath.Join(s.relationRoot, item.Name())
		if err := readJSON(path, maxRelationFileBytes, &relation); err != nil || relation.RecordingID != id || !watchIDPattern.MatchString(relation.WatchID) || relation.PartIndex < 1 {
			return errors.New("persisted watch relation is invalid")
		}
		if _, exists := s.document.Definitions[relation.WatchID]; !exists {
			if err := removeRegular(path); err != nil {
				return err
			}
			continue
		}
		s.document.Relations[id] = relation
	}
	if len(s.document.Relations) > MaxRelations {
		oldest := oldestRelationID(s.document.Relations)
		if relation, ok := s.document.Relations[oldest]; ok {
			if err := s.markRelationHistoryTruncated(relation.WatchID); err != nil {
				return err
			}
		}
		if err := removeRegular(filepath.Join(s.relationRoot, oldest+".json")); err != nil {
			return err
		}
		delete(s.document.Relations, oldest)
	}
	return nil
}

func (s *Store) List() []Definition {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Definition, 0, len(s.document.Definitions))
	for _, item := range s.document.Definitions {
		out = append(out, cloneDefinition(item))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

// Snapshots returns the bounded in-memory definition/runtime index without
// reading secret files. The scheduler uses this on every inexpensive tick.
func (s *Store) Snapshots() []Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Snapshot, 0, len(s.document.Definitions))
	for id, definition := range s.document.Definitions {
		out = append(out, Snapshot{Definition: cloneDefinition(definition), Runtime: cloneRuntime(s.document.Runtimes[id])})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Definition.CreatedAt.Before(out[j].Definition.CreatedAt) })
	return out
}

// DueIDs returns only scheduler-eligible Watch identifiers. Unlike Snapshots,
// it does not clone potentially large input objects or touch secret files, so
// a frequent scheduler tick stays bounded even when no Watch is due.
func (s *Store) DueIDs(now time.Time) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0)
	for id, def := range s.document.Definitions {
		if !def.Enabled {
			continue
		}
		runtime := s.document.Runtimes[id]
		if runtime.ActiveRecordingID != "" || runtime.PendingRecordingID != "" {
			continue
		}
		if runtime.NextCheckAt != nil && now.Before(*runtime.NextCheckAt) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (s *Store) SecretConfigured(id string) map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]bool{}
	for key, configured := range s.secretSet[id] {
		out[key] = configured
	}
	return out
}

func (s *Store) Get(id string) (Definition, Runtime, map[string]string, error) {
	if !watchIDPattern.MatchString(id) {
		return Definition{}, Runtime{}, nil, ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	definition, ok := s.document.Definitions[id]
	if !ok {
		return Definition{}, Runtime{}, nil, ErrNotFound
	}
	secrets, err := readSecrets(s.secretPath(id))
	if err != nil {
		return Definition{}, Runtime{}, nil, err
	}
	return cloneDefinition(definition), cloneRuntime(s.document.Runtimes[id]), secrets, nil
}

func (s *Store) Secrets(id string) (map[string]string, error) {
	if !watchIDPattern.MatchString(id) {
		return nil, ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.document.Definitions[id]; !ok {
		return nil, ErrNotFound
	}
	return readSecrets(s.secretPath(id))
}

func (s *Store) Create(def Definition, secrets map[string]string, runtime Runtime) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.document.Definitions) >= MaxWatches {
		return errors.New("watch limit reached")
	}
	if secrets == nil {
		secrets = map[string]string{}
	}
	if !watchIDPattern.MatchString(def.ID) || def.AdapterID == "" || len(def.Input) == 0 || len(def.Input) > MaxInputBytes || !validSecrets(secrets) || !validRuntime(runtime) {
		return ErrInvalid
	}
	if _, exists := s.document.Definitions[def.ID]; exists {
		return ErrConflict
	}
	if s.secretBytes+secretSize(secrets) > MaxAggregateSecretBytes {
		return errors.New("aggregate watch secrets exceed configured bound")
	}
	if err := writeJSONAtomic(s.secretPath(def.ID), secrets, MaxSecretFileBytes, 0o600); err != nil {
		return err
	}
	if err := writeJSONAtomic(s.runtimePath(def.ID), runtime, maxRuntimeFileBytes, 0o600); err != nil {
		_ = removeRegular(s.secretPath(def.ID))
		return err
	}
	if err := writeJSONAtomic(s.eventPath(def.ID), []Event{}, maxEventFileBytes, 0o600); err != nil {
		_ = removeRegular(s.runtimePath(def.ID))
		_ = removeRegular(s.secretPath(def.ID))
		return err
	}
	// Definition is the commit marker. Startup removes sidecar files that have
	// no corresponding definition after an interrupted create.
	if err := writeJSONAtomic(s.definitionPath(def.ID), def, maxDefinitionFileBytes, 0o600); err != nil {
		_ = removeRegular(s.eventPath(def.ID))
		_ = removeRegular(s.runtimePath(def.ID))
		_ = removeRegular(s.secretPath(def.ID))
		_ = syncDir(s.secretRoot)
		return err
	}
	s.document.Definitions[def.ID] = cloneDefinition(def)
	s.document.Runtimes[def.ID] = cloneRuntime(runtime)
	s.document.Events[def.ID] = []Event{}
	s.secretBytes += secretSize(secrets)
	s.secretSet[def.ID] = configuredFlags(secrets)
	return nil
}

func (s *Store) Update(def Definition, secrets map[string]string, replaceSecrets bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.document.Definitions[def.ID]
	if !ok {
		return ErrNotFound
	}
	if def.AdapterID == "" || len(def.Input) == 0 || len(def.Input) > MaxInputBytes {
		return ErrInvalid
	}
	previousSecrets := map[string]string{}
	if replaceSecrets {
		if !validSecrets(secrets) {
			return ErrInvalid
		}
		previousSecrets, _ = readSecrets(s.secretPath(def.ID))
		if s.secretBytes-secretSize(previousSecrets)+secretSize(secrets) > MaxAggregateSecretBytes {
			return errors.New("aggregate watch secrets exceed configured bound")
		}
		if err := writeJSONAtomic(s.secretPath(def.ID), secrets, MaxSecretFileBytes, 0o600); err != nil {
			return err
		}
	}
	if err := writeJSONAtomic(s.definitionPath(def.ID), def, maxDefinitionFileBytes, 0o600); err != nil {
		if replaceSecrets {
			_ = writeJSONAtomic(s.secretPath(def.ID), previousSecrets, MaxSecretFileBytes, 0o600)
		}
		return err
	}
	s.document.Definitions[def.ID] = cloneDefinition(def)
	if replaceSecrets {
		s.secretBytes += secretSize(secrets) - secretSize(previousSecrets)
		s.secretSet[def.ID] = configuredFlags(secrets)
	}
	return nil
}

func (s *Store) SaveRuntime(id string, runtime Runtime) error {
	if !watchIDPattern.MatchString(id) || !validRuntime(runtime) {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.document.Definitions[id]; !ok {
		return ErrNotFound
	}
	if err := writeJSONAtomic(s.runtimePath(id), runtime, maxRuntimeFileBytes, 0o600); err != nil {
		return err
	}
	s.document.Runtimes[id] = cloneRuntime(runtime)
	return nil
}

func (s *Store) AddEvent(id string, event Event) error {
	if !watchIDPattern.MatchString(id) || !validEvent(event) {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.document.Definitions[id]; !ok {
		return ErrNotFound
	}
	if event.ID == "" {
		generated, err := newID()
		if err != nil {
			return err
		}
		event.ID = generated
	}
	if event.State == "" {
		event.State = s.document.Runtimes[id].State
	}
	previous := cloneEventMap(s.document.Events)
	list := append(append([]Event(nil), s.document.Events[id]...), event)
	if len(list) > MaxEventsPerWatch {
		list = append([]Event(nil), list[len(list)-MaxEventsPerWatch:]...)
	}
	s.document.Events[id] = list
	trimEvents(s.document.Events, MaxEventsTotal)
	if err := s.persistEventChanges(previous); err != nil {
		s.document.Events = previous
		return err
	}
	return nil
}

func (s *Store) Events(id string, limit int) ([]Event, error) {
	if !watchIDPattern.MatchString(id) {
		return nil, ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.document.Definitions[id]; !ok {
		return nil, ErrNotFound
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > MaxEventsPerWatch {
		limit = MaxEventsPerWatch
	}
	items := append([]Event(nil), s.document.Events[id]...)
	if len(items) > limit {
		items = items[len(items)-limit:]
	}
	for left, right := 0, len(items)-1; left < right; left, right = left+1, right-1 {
		items[left], items[right] = items[right], items[left]
	}
	return items, nil
}

func (s *Store) Delete(id string) error {
	if !watchIDPattern.MatchString(id) {
		return ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.document.Definitions[id]; !ok {
		return ErrNotFound
	}
	// Removing the definition is the logical delete. Sidecar cleanup is safe
	// to retry on startup and never touches canonical recordings.
	if err := removeRegularNoSync(s.definitionPath(id)); err != nil {
		return err
	}
	delete(s.document.Definitions, id)
	delete(s.document.Runtimes, id)
	delete(s.document.Events, id)
	delete(s.secretSet, id)
	if secrets, err := readSecrets(s.secretPath(id)); err == nil {
		s.secretBytes -= secretSize(secrets)
	}
	cleanupErrors := []error{}
	for _, path := range []string{s.runtimePath(id), s.eventPath(id), s.secretPath(id)} {
		if err := removeRegularNoSync(path); err != nil {
			cleanupErrors = append(cleanupErrors, err)
		}
	}
	for recordingID, relation := range s.document.Relations {
		if relation.WatchID == id {
			if err := removeRegularNoSync(s.relationPath(recordingID)); err != nil {
				cleanupErrors = append(cleanupErrors, err)
			}
			delete(s.document.Relations, recordingID)
		}
	}
	cleanupErrors = append(cleanupErrors,
		syncDir(s.definitionRoot),
		syncDir(s.runtimeRoot),
		syncDir(s.eventRoot),
		syncDir(s.secretRoot),
		syncDir(s.relationRoot),
	)
	return errors.Join(cleanupErrors...)
}

func (s *Store) SaveRelation(relation RecordingRelation) error {
	if !watchIDPattern.MatchString(relation.RecordingID) || !watchIDPattern.MatchString(relation.WatchID) || relation.PartIndex < 1 {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.document.Definitions[relation.WatchID]; !ok {
		return ErrNotFound
	}
	previous, existed := s.document.Relations[relation.RecordingID]
	removedID := ""
	var removed RecordingRelation
	if !existed && len(s.document.Relations) >= MaxRelations {
		removedID = oldestRelationID(s.document.Relations)
		removed = s.document.Relations[removedID]
		if removedID != "" {
			if err := s.markRelationHistoryTruncated(removed.WatchID); err != nil {
				return err
			}
			if err := removeRegular(s.relationPath(removedID)); err != nil {
				return err
			}
			delete(s.document.Relations, removedID)
		}
	}
	if err := writeJSONAtomic(s.relationPath(relation.RecordingID), relation, maxRelationFileBytes, 0o600); err != nil {
		if existed {
			s.document.Relations[relation.RecordingID] = previous
		} else {
			delete(s.document.Relations, relation.RecordingID)
		}
		if removedID != "" {
			_ = writeJSONAtomic(s.relationPath(removedID), removed, maxRelationFileBytes, 0o600)
			s.document.Relations[removedID] = removed
		}
		return err
	}
	s.document.Relations[relation.RecordingID] = relation
	return nil
}

// DeleteRelation removes one Watch association without touching its recording.
// It is used when a preallocated start intent did not produce an archive.
func (s *Store) DeleteRelation(recordingID string) error {
	if !watchIDPattern.MatchString(recordingID) {
		return ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.document.Relations[recordingID]; !exists {
		return nil
	}
	if err := removeRegular(s.relationPath(recordingID)); err != nil {
		return err
	}
	delete(s.document.Relations, recordingID)
	return nil
}

func (s *Store) Relation(recordingID string) (RecordingRelation, error) {
	if !watchIDPattern.MatchString(recordingID) {
		return RecordingRelation{}, ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	relation, ok := s.document.Relations[recordingID]
	if !ok {
		return RecordingRelation{}, ErrNotFound
	}
	return relation, nil
}

func (s *Store) PartCount(watchID, sessionDigest string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, relation := range s.document.Relations {
		if relation.WatchID == watchID && relation.SessionDigest == sessionDigest {
			count++
		}
	}
	return count
}

func (s *Store) RelationsForWatch(id string, limit int) ([]RecordingRelation, error) {
	items, _, err := s.RelationsForWatchPage(id, limit)
	return items, err
}

func (s *Store) RelationsForWatchPage(id string, limit int) ([]RecordingRelation, bool, error) {
	if !watchIDPattern.MatchString(id) {
		return nil, false, ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	definition, ok := s.document.Definitions[id]
	if !ok {
		return nil, false, ErrNotFound
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > MaxListLimit {
		limit = MaxListLimit
	}
	items := []RecordingRelation{}
	for _, relation := range s.document.Relations {
		if relation.WatchID == id {
			items = append(items, relation)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	truncated := definition.RecordingHistoryTruncated || len(items) > limit
	if len(items) > limit {
		items = items[:limit]
	}
	return items, truncated, nil
}

func (s *Store) markRelationHistoryTruncated(watchID string) error {
	definition, ok := s.document.Definitions[watchID]
	if !ok || definition.RecordingHistoryTruncated {
		return nil
	}
	definition.RecordingHistoryTruncated = true
	if err := writeJSONAtomic(s.definitionPath(watchID), definition, maxDefinitionFileBytes, 0o600); err != nil {
		return err
	}
	s.document.Definitions[watchID] = definition
	return nil
}

func (s *Store) Relations() []RecordingRelation {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]RecordingRelation, 0, len(s.document.Relations))
	for _, relation := range s.document.Relations {
		items = append(items, relation)
	}
	return items
}

func (s *Store) definitionPath(id string) string { return filepath.Join(s.definitionRoot, id+".json") }
func (s *Store) runtimePath(id string) string    { return filepath.Join(s.runtimeRoot, id+".json") }
func (s *Store) eventPath(id string) string      { return filepath.Join(s.eventRoot, id+".json") }
func (s *Store) secretPath(id string) string     { return filepath.Join(s.secretRoot, id+".json") }
func (s *Store) relationPath(id string) string   { return filepath.Join(s.relationRoot, id+".json") }

func (s *Store) persistEventChanges(previous map[string][]Event) error {
	changed := []string{}
	for id, list := range s.document.Events {
		if !eventsEqual(previous[id], list) {
			changed = append(changed, id)
		}
	}
	for id := range previous {
		if _, exists := s.document.Events[id]; !exists {
			changed = append(changed, id)
		}
	}
	written := []string{}
	for _, id := range changed {
		list, exists := s.document.Events[id]
		if !exists || len(list) == 0 {
			if err := removeRegular(s.eventPath(id)); err != nil {
				return s.rollbackEventFiles(previous, written, err)
			}
		} else if err := writeJSONAtomic(s.eventPath(id), list, maxEventFileBytes, 0o600); err != nil {
			return s.rollbackEventFiles(previous, written, err)
		}
		written = append(written, id)
	}
	return syncDir(s.eventRoot)
}

func (s *Store) rollbackEventFiles(previous map[string][]Event, written []string, cause error) error {
	for _, id := range written {
		if list, exists := previous[id]; exists && len(list) > 0 {
			_ = writeJSONAtomic(s.eventPath(id), list, maxEventFileBytes, 0o600)
		} else {
			_ = removeRegular(s.eventPath(id))
		}
	}
	return cause
}

func eventsEqual(left, right []Event) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func cloneEventMap(events map[string][]Event) map[string][]Event {
	copy := make(map[string][]Event, len(events))
	for id, list := range events {
		copy[id] = append([]Event(nil), list...)
	}
	return copy
}

func secretSize(secrets map[string]string) int64 {
	var total int64
	for key, value := range secrets {
		total += int64(len(key) + len(value))
	}
	return total
}

func configuredFlags(secrets map[string]string) map[string]bool {
	flags := make(map[string]bool, len(secrets))
	for key, value := range secrets {
		flags[key] = value != ""
	}
	return flags
}

func oldestRelationID(relations map[string]RecordingRelation) string {
	oldestID := ""
	var oldest time.Time
	for id, relation := range relations {
		if oldestID == "" || relation.CreatedAt.Before(oldest) {
			oldestID, oldest = id, relation.CreatedAt
		}
	}
	return oldestID
}

func jsonFiles(dir string, limit int) ([]os.DirEntry, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]os.DirEntry, 0, len(entries))
	removedTemp := false
	for _, item := range entries {
		name := item.Name()
		if strings.HasPrefix(name, ".watch-") && strings.HasSuffix(name, ".tmp") {
			if item.Type()&os.ModeSymlink != 0 || item.IsDir() {
				return nil, errors.New("unsafe temporary watch file")
			}
			if err := removeRegular(filepath.Join(dir, name)); err != nil {
				return nil, err
			}
			removedTemp = true
			continue
		}
		if !strings.HasSuffix(name, ".json") || !watchIDPattern.MatchString(strings.TrimSuffix(name, ".json")) || item.Type()&os.ModeSymlink != 0 || item.IsDir() {
			return nil, errors.New("unexpected or unsafe watch file")
		}
		files = append(files, item)
	}
	if len(files) > limit {
		return nil, errors.New("watch file count exceeds configured bound")
	}
	if removedTemp {
		if err := syncDir(dir); err != nil {
			return nil, err
		}
	}
	return files, nil
}

func readJSON(path string, limit int64, destination any) error {
	data, err := safeRead(path, limit)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}

func removeRegular(path string) error {
	if err := removeRegularNoSync(path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func removeRegularNoSync(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe watch file")
	}
	return os.Remove(path)
}

func readSecrets(path string) (map[string]string, error) {
	data, err := safeRead(path, MaxSecretFileBytes)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	values := map[string]string{}
	if json.Unmarshal(data, &values) != nil || values == nil || !validSecrets(values) {
		return nil, errors.New("invalid watch secrets")
	}
	return values, nil
}

func validSecrets(values map[string]string) bool {
	if len(values) > 32 {
		return false
	}
	total := 0
	for key, value := range values {
		if key == "" || len(key) > 128 || len(value) > MaxSecretBytes || !utf8.ValidString(value) {
			return false
		}
		total += len(key) + len(value)
		if total > MaxSecretBytes {
			return false
		}
	}
	return true
}

func validRuntime(runtime Runtime) bool {
	if runtime.FailureCount < 0 || runtime.FailureCount > 16 {
		return false
	}
	switch runtime.State {
	case "", StateDisabled, StateOffline, StateChecking, StateStarting, StateRecording, StateBackoff, StateAttentionRequired, StateSuppressed:
	default:
		return false
	}
	for _, digest := range []string{runtime.SessionDigest, runtime.SuppressedSessionDigest} {
		if digest != "" && (len(digest) != 64 || strings.Trim(digest, "0123456789abcdef") != "") {
			return false
		}
	}
	return (runtime.ActiveRecordingID == "" || watchIDPattern.MatchString(runtime.ActiveRecordingID)) && (runtime.PendingRecordingID == "" || watchIDPattern.MatchString(runtime.PendingRecordingID))
}

func validEvent(event Event) bool {
	switch event.Type {
	case "watch_created", "watch_updated", "watch_enabled", "watch_disabled", "live_detected", "recording_started", "recording_ended", "check_failed", "attention_required", "watch_deleted":
	default:
		return false
	}
	if event.Code != "" {
		switch event.Code {
		case "adapter_unavailable", "adapter_disabled", "check_failed", "resolve_unavailable", "invalid_response", "start_failed", "storage_failed", "start_recovery_ambiguous", "authentication_required", "interaction_required", "configuration_required":
		default:
			return false
		}
	}
	return event.RecordingID == "" || watchIDPattern.MatchString(event.RecordingID)
}

func totalEvents(events map[string][]Event) int {
	total := 0
	for _, list := range events {
		total += len(list)
	}
	return total
}
func trimEvents(events map[string][]Event, maximum int) {
	type indexed struct {
		id    string
		index int
		at    time.Time
	}
	all := []indexed{}
	for id, list := range events {
		for i, item := range list {
			all = append(all, indexed{id: id, index: i, at: item.At})
		}
	}
	if len(all) <= maximum {
		return
	}
	sort.Slice(all, func(i, j int) bool { return all[i].at.Before(all[j].at) })
	drop := map[string]map[int]bool{}
	for _, item := range all[:len(all)-maximum] {
		if drop[item.id] == nil {
			drop[item.id] = map[int]bool{}
		}
		drop[item.id][item.index] = true
	}
	for id, indexes := range drop {
		kept := make([]Event, 0, len(events[id])-len(indexes))
		for i, item := range events[id] {
			if !indexes[i] {
				kept = append(kept, item)
			}
		}
		events[id] = kept
	}
}

func privateDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe watch storage directory")
	}
	return os.Chmod(path, 0o700)
}
func safeRead(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > limit {
		return nil, errors.New("unsafe or oversized watch file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, errors.New("watch file exceeds limit")
	}
	return data, nil
}
func writeJSONAtomic(path string, value any, limit int, mode os.FileMode) error {
	data, err := json.Marshal(value)
	if err != nil || len(data) > limit {
		return errors.New("watch file exceeds limit")
	}
	return writeBytesAtomic(path, data, mode)
}
func writeBytesAtomic(path string, data []byte, mode os.FileMode) error {
	if len(data) > maxStoreBytes {
		return errors.New("watch file exceeds limit")
	}
	dir := filepath.Dir(path)
	if err := privateDir(dir); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return errors.New("unsafe watch file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".watch-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	keep := false
	defer func() {
		_ = tmp.Close()
		if !keep {
			_ = os.Remove(name)
		}
	}()
	if err = tmp.Chmod(mode); err == nil {
		_, err = tmp.Write(data)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	if err = syncDir(dir); err != nil {
		return err
	}
	keep = true
	return nil
}
func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func ensureJSONEOF(d *json.Decoder) error {
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func cloneDefinition(item Definition) Definition {
	item.Input = append(json.RawMessage(nil), item.Input...)
	item.Resource = cloneRef(item.Resource)
	return item
}
func cloneRuntime(item Runtime) Runtime {
	if item.LastCheckAt != nil {
		v := *item.LastCheckAt
		item.LastCheckAt = &v
	}
	if item.NextCheckAt != nil {
		v := *item.NextCheckAt
		item.NextCheckAt = &v
	}
	if item.StartingAt != nil {
		v := *item.StartingAt
		item.StartingAt = &v
	}
	return item
}
func cloneRef(ref *adapterproto.ResourceRef) *adapterproto.ResourceRef {
	if ref == nil {
		return nil
	}
	copy := *ref
	copy.Parent = cloneRef(ref.Parent)
	return &copy
}
func cloneDocument(doc storeDocument) storeDocument {
	out := emptyDocument()
	for k, v := range doc.Definitions {
		out.Definitions[k] = cloneDefinition(v)
	}
	for k, v := range doc.Runtimes {
		out.Runtimes[k] = cloneRuntime(v)
	}
	for k, v := range doc.Events {
		out.Events[k] = append([]Event(nil), v...)
	}
	for k, v := range doc.Relations {
		out.Relations[k] = v
	}
	return out
}
