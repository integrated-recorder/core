// Package management stores bounded product-management projections separately
// from canonical recording archives.
package management

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	maxProjectionFileBytes = 16 << 20
	maxWorkflowHistory     = 5000
	maxRecordingEvents     = 2000
	maxAuditEvents         = 10000
	maxNotifications       = 5000
	maxDisabledAdapters    = 512
	defaultQueryLimit      = 100
)

var (
	recordingIDRE = regexp.MustCompile(`^[a-f0-9]{32}$`)
	identifierRE  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
)

// Store persists UI-facing projections without changing canonical recording
// metadata or payloads. Every operation is serialized to keep JSON snapshots
// atomic and prevent lost updates.
type Store struct {
	root string
	mu   sync.Mutex

	tags             map[string][]string
	recordingEvents  map[string][]RecordingEvent
	forgetting       map[string]bool
	disabledAdapters map[string]struct{}
	workflowHistory  []WorkflowHistoryEvent
	audit            []AuditEvent
	notifications    []Notification
}

type tagsDocument struct {
	Tags []string `json:"tags"`
}

type deletionMarker struct {
	RecordingID string `json:"recording_id"`
}

// Open opens the management projection store below root (normally DATA_DIR).
// Existing malformed or unsafe projection files fail initialization without
// touching canonical recording data.
func Open(root string) (*Store, error) {
	if strings.TrimSpace(root) == "" {
		return nil, fmt.Errorf("management data directory is empty")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve management data directory: %w", err)
	}
	managementRoot := filepath.Join(abs, "management")
	if err := ensurePrivateDir(managementRoot); err != nil {
		return nil, fmt.Errorf("prepare management directory: %w", err)
	}
	for _, name := range []string{"recordings", "recording-events", filepath.Join("diagnostics", "recordings")} {
		if err := ensurePrivateDir(filepath.Join(managementRoot, name)); err != nil {
			return nil, fmt.Errorf("prepare management %s directory: %w", name, err)
		}
	}
	if err := ensurePrivateDir(filepath.Join(managementRoot, "deletions")); err != nil {
		return nil, fmt.Errorf("prepare management deletions directory: %w", err)
	}
	if err := syncDirectory(managementRoot); err != nil {
		return nil, fmt.Errorf("sync management directory: %w", err)
	}
	if err := syncDirectory(abs); err != nil {
		return nil, fmt.Errorf("sync data directory: %w", err)
	}
	for _, dir := range []string{managementRoot, filepath.Join(managementRoot, "recordings"), filepath.Join(managementRoot, "recording-events"), filepath.Join(managementRoot, "deletions")} {
		if err := cleanupAtomicTemps(dir); err != nil {
			return nil, fmt.Errorf("clean management temporary files: %w", err)
		}
	}

	s := &Store{
		root:             managementRoot,
		tags:             map[string][]string{},
		recordingEvents:  map[string][]RecordingEvent{},
		forgetting:       map[string]bool{},
		workflowHistory:  []WorkflowHistoryEvent{},
		audit:            []AuditEvent{},
		notifications:    []Notification{},
		disabledAdapters: map[string]struct{}{},
	}
	if err := s.recoverPendingDeletions(); err != nil {
		return nil, fmt.Errorf("recover management deletions: %w", err)
	}
	if err := s.loadAll(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) loadAll() error {
	if err := s.loadPerRecording(filepath.Join(s.root, "recordings"), func(id string, data []byte) error {
		var doc tagsDocument
		if err := decodeStrict(data, &doc); err != nil {
			return fmt.Errorf("decode tags: %w", err)
		}
		normalized, err := normalizeTags(doc.Tags)
		if err != nil {
			return fmt.Errorf("validate tags: %w", err)
		}
		if !equalStrings(normalized, doc.Tags) {
			return fmt.Errorf("tags are not in canonical order")
		}
		s.tags[id] = normalized
		return nil
	}); err != nil {
		return fmt.Errorf("load management tags: %w", err)
	}
	if err := s.loadPerRecording(filepath.Join(s.root, "recording-events"), func(id string, data []byte) error {
		var events []RecordingEvent
		if err := decodeStrict(data, &events); err != nil {
			return fmt.Errorf("decode recording events: %w", err)
		}
		if len(events) > maxRecordingEvents {
			return fmt.Errorf("recording event list exceeds limit")
		}
		for _, event := range events {
			if event.RecordingID != id {
				return fmt.Errorf("recording event identity mismatch")
			}
			if err := validateRecordingEvent(event); err != nil {
				return err
			}
		}
		if hasDuplicateRecordingEventIDs(events) {
			return fmt.Errorf("duplicate recording event identifier")
		}
		s.recordingEvents[id] = sortRecordingEvents(events)
		return nil
	}); err != nil {
		return fmt.Errorf("load recording events: %w", err)
	}
	if err := s.loadGlobal("workflow-history.json", &s.workflowHistory); err != nil {
		return fmt.Errorf("load workflow history: %w", err)
	}
	if s.workflowHistory == nil {
		s.workflowHistory = []WorkflowHistoryEvent{}
	}
	if len(s.workflowHistory) > maxWorkflowHistory {
		return fmt.Errorf("load workflow history: event list exceeds limit")
	}
	for _, event := range s.workflowHistory {
		if err := validateWorkflowHistoryEvent(event); err != nil {
			return fmt.Errorf("load workflow history: %w", err)
		}
	}
	if hasDuplicateWorkflowEventIDs(s.workflowHistory) {
		return fmt.Errorf("load workflow history: duplicate event identifier")
	}
	s.workflowHistory = sortWorkflowHistory(s.workflowHistory)
	if err := s.loadGlobal("audit.json", &s.audit); err != nil {
		return fmt.Errorf("load audit events: %w", err)
	}
	if s.audit == nil {
		s.audit = []AuditEvent{}
	}
	if len(s.audit) > maxAuditEvents {
		return fmt.Errorf("load audit events: event list exceeds limit")
	}
	for _, event := range s.audit {
		if err := validateAuditEvent(event); err != nil {
			return fmt.Errorf("load audit events: %w", err)
		}
	}
	if hasDuplicateAuditIDs(s.audit) {
		return fmt.Errorf("load audit events: duplicate event identifier")
	}
	s.audit = sortAudit(s.audit)
	if err := s.loadGlobal("notifications.json", &s.notifications); err != nil {
		return fmt.Errorf("load notifications: %w", err)
	}
	if s.notifications == nil {
		s.notifications = []Notification{}
	}
	if len(s.notifications) > maxNotifications {
		return fmt.Errorf("load notifications: event list exceeds limit")
	}
	for _, notification := range s.notifications {
		if err := validateNotification(notification); err != nil {
			return fmt.Errorf("load notifications: %w", err)
		}
	}
	if hasDuplicateNotificationIDs(s.notifications) {
		return fmt.Errorf("load notifications: duplicate notification identifier")
	}
	s.notifications = sortNotifications(s.notifications)
	if err := s.loadAdapterPreferences(); err != nil {
		return fmt.Errorf("load adapter preferences: %w", err)
	}
	return nil
}

func (s *Store) recoverPendingDeletions() error {
	dir := filepath.Join(s.root, "deletions")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") {
			return fmt.Errorf("unexpected deletion marker")
		}
		id := strings.TrimSuffix(name, ".json")
		if !recordingIDRE.MatchString(id) {
			return fmt.Errorf("invalid deletion marker identifier")
		}
		markerPath := filepath.Join(dir, name)
		data, err := readProjection(markerPath)
		if err != nil {
			return fmt.Errorf("read deletion marker: %w", err)
		}
		var marker deletionMarker
		if err := decodeStrict(data, &marker); err != nil || marker.RecordingID != id {
			return fmt.Errorf("invalid deletion marker contents")
		}
		if err := s.removeRecordingProjections(id); err != nil {
			return fmt.Errorf("finish pending management deletion: %w", err)
		}
		if err := os.Remove(markerPath); err != nil {
			return fmt.Errorf("remove deletion marker: %w", err)
		}
		if err := syncDirectory(dir); err != nil {
			return fmt.Errorf("sync deletion marker directory: %w", err)
		}
	}
	return nil
}

func (s *Store) removeRecordingProjections(id string) error {
	if err := removeThroughTombstone(s.recordingPath(id), id+"-tags"); err != nil {
		return err
	}
	if err := removeThroughTombstone(s.recordingEventsPath(id), id+"-events"); err != nil {
		return err
	}
	if err := removeThroughTombstone(filepath.Join(s.root, "diagnostics", "recordings", id+".json"), id+"-storage-diagnostic"); err != nil {
		return err
	}
	return nil
}

func removeThroughTombstone(path, suffix string) error {
	dir := filepath.Dir(path)
	tombstone := filepath.Join(dir, ".projection-delete-"+suffix+".tmp")
	if err := removeRegularFileIfPresent(tombstone); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("projection is not a regular file")
	}
	if err := os.Rename(path, tombstone); err != nil {
		return err
	}
	if err := syncDirectory(dir); err != nil {
		return err
	}
	if err := os.Remove(tombstone); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func removeRegularFileIfPresent(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("projection tombstone is not a regular file")
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func (s *Store) loadGlobal(name string, target any) error {
	path := filepath.Join(s.root, name)
	data, err := readProjection(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return decodeStrict(data, target)
}

func (s *Store) loadPerRecording(dir string, consume func(string, []byte) error) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") {
			return fmt.Errorf("unexpected management file %q", name)
		}
		id := strings.TrimSuffix(name, ".json")
		if !recordingIDRE.MatchString(id) {
			return fmt.Errorf("invalid management recording identifier")
		}
		data, err := readProjection(filepath.Join(dir, name))
		if err != nil {
			return fmt.Errorf("read %s: %w", name, err)
		}
		if err := consume(id, data); err != nil {
			return fmt.Errorf("validate %s: %w", name, err)
		}
	}
	return nil
}

func readProjection(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("projection is not a regular file")
	}
	if info.Size() > maxProjectionFileBytes {
		return nil, fmt.Errorf("projection exceeds size limit")
	}
	if err := os.Chmod(path, 0600); err != nil {
		return nil, fmt.Errorf("restrict projection permissions: %w", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxProjectionFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxProjectionFileBytes {
		return nil, fmt.Errorf("projection exceeds size limit")
	}
	return data, nil
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func ensurePrivateDir(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("management path is not a real directory")
	}
	return os.Chmod(path, 0700)
}

func cleanupAtomicTemps(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, ".projection-") || !strings.HasSuffix(name, ".tmp") {
			continue
		}
		path := filepath.Join(dir, name)
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe temporary projection entry")
		}
		if err := os.Remove(path); err != nil {
			return err
		}
		removed = true
	}
	if removed {
		return syncDirectory(dir)
	}
	return nil
}

func (s *Store) write(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if len(data) > maxProjectionFileBytes {
		return fmt.Errorf("management projection exceeds size limit")
	}
	return atomicWrite(path, data)
}

func atomicWrite(path string, data []byte) (retErr error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".projection-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()
	if err = tmp.Chmod(0600); err == nil {
		_, err = tmp.Write(data)
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmpName, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func validRecordingID(id string) error {
	if !recordingIDRE.MatchString(id) {
		return fmt.Errorf("invalid recording identifier")
	}
	return nil
}

func validIdentifier(value string, max int) bool {
	return len(value) <= max && utf8.ValidString(value) && identifierRE.MatchString(value)
}

func limitFor(limit, maximum int) int {
	if limit <= 0 {
		return defaultQueryLimit
	}
	if limit > maximum {
		return maximum
	}
	return limit
}

func sortByTime[T interface {
	eventTime() time.Time
	eventID() string
}](items []T) {
	sort.Slice(items, func(i, j int) bool {
		if !items[i].eventTime().Equal(items[j].eventTime()) {
			return items[i].eventTime().After(items[j].eventTime())
		}
		return items[i].eventID() > items[j].eventID()
	})
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
