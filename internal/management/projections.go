package management

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

var (
	ErrDuplicateNotification = errors.New("duplicate notification identifier")
	ErrInvalidAuditCursor    = errors.New("invalid audit cursor")
)

// ResourceRef is a deliberately small, opaque resource reference. It contains
// no adapter attributes, configuration, credentials, or source URLs.
type ResourceRef struct {
	Type   string       `json:"resource_type"`
	ID     string       `json:"resource_id"`
	Parent *ResourceRef `json:"parent,omitempty"`
}

// ChallengeSummary is safe workflow history metadata; answer values are not
// represented by this type and therefore cannot be persisted here.
type ChallengeSummary struct {
	Title           string `json:"title,omitempty"`
	Message         string `json:"message,omitempty"`
	FieldCount      int    `json:"field_count"`
	HasSecretFields bool   `json:"has_secret_fields"`
}

// WorkflowHistoryEvent records a bounded, answer-free workflow transition.
type WorkflowHistoryEvent struct {
	ID         string            `json:"id"`
	WorkflowID string            `json:"workflow_id"`
	AdapterID  string            `json:"adapter_id"`
	State      string            `json:"state"`
	At         time.Time         `json:"at"`
	Resource   *ResourceRef      `json:"resource,omitempty"`
	Challenge  *ChallengeSummary `json:"challenge,omitempty"`
}

// RecordingEvent stores low-volume, safe recording timeline events. Message
// accepts only a short allowlist of fixed strings; arbitrary errors and source
// URLs must be recorded elsewhere only after safe summarization.
type RecordingEvent struct {
	ID          string    `json:"id"`
	RecordingID string    `json:"recording_id"`
	Type        string    `json:"type"`
	At          time.Time `json:"at"`
	Count       int       `json:"count,omitempty"`
	Message     string    `json:"message,omitempty"`
}

// AuditEvent contains only a type and opaque object reference, never arbitrary
// request payloads or user-provided secret values.
type AuditEvent struct {
	ID       string      `json:"id"`
	Type     string      `json:"type"`
	At       time.Time   `json:"at"`
	ObjectID string      `json:"object_id,omitempty"`
	Actor    *AuditActor `json:"actor,omitempty"`
}

// AuditActor identifies the control-plane principal that caused an event.
// Canonical archive provenance does not use this type.
type AuditActor struct {
	Type   string `json:"type"`
	UserID string `json:"user_id,omitempty"`
}

const (
	AuditActorUser      = "user"
	AuditActorSystem    = "system"
	auditCursorVersion  = 1
	maxAuditCursorBytes = 256
	maxAuditPageSize    = 500
)

type auditCursor struct {
	Version int       `json:"v"`
	At      time.Time `json:"at"`
	ID      string    `json:"id"`
}

// Notification is a bounded, user-facing event with an explicit read state.
type Notification struct {
	ID       string    `json:"id"`
	Type     string    `json:"type"`
	At       time.Time `json:"at"`
	Read     bool      `json:"read"`
	ObjectID string    `json:"object_id,omitempty"`
}

var workflowStates = map[string]struct{}{
	"started": {}, "challenge_required": {}, "continued": {}, "resolved": {},
	"canceled": {}, "expired": {}, "failed": {},
}

var recordingEventTypes = map[string]struct{}{
	"recording_started": {}, "manifest_observed": {}, "source_refreshed": {},
	"segment_retry": {}, "gap_detected": {}, "gap_committed": {},
	"recording_completed": {}, "recording_stopped": {}, "recording_interrupted": {},
	"integrity_started": {}, "integrity_completed": {}, "export_started": {}, "export_completed": {},
	"export_failed": {}, "export_canceled": {},
}

var safeRecordingMessages = map[string]struct{}{
	"manifest observed": {}, "source refreshed": {}, "segment retry scheduled": {},
	"gap detected": {}, "gap committed": {}, "recording completed": {},
	"recording stopped": {}, "recording interrupted": {}, "integrity verification started": {},
	"integrity verification completed": {}, "export started": {}, "export completed": {},
	"export failed": {}, "export canceled": {},
}

var auditTypes = map[string]struct{}{
	"recording_deleted": {}, "config_changed": {}, "adapter_restarted": {}, "adapter_enabled": {},
	"adapter_disabled": {}, "integrity_requested": {}, "export_requested": {},
	"recording_tags_updated": {},
	"watch_created":          {}, "watch_updated": {}, "watch_enabled": {}, "watch_disabled": {},
	"watch_deleted": {}, "manual_watch_check": {},
	"runtime_update_staged": {}, "runtime_update_activated": {}, "runtime_update_rolled_back": {},
	"adapter_reconciled": {}, "plugin_registry_refreshed": {}, "plugin_installed": {},
	"plugin_updated": {}, "plugin_uninstalled": {},
	"storage_instance_created": {}, "storage_instance_configured": {}, "storage_instance_probed": {},
	"storage_instance_activated": {}, "storage_provider_configured": {}, "storage_provider_probed": {},
	"storage_provider_activated": {}, "installation_setup_begun": {}, "installation_setup_completed": {},
}

var notificationTypes = map[string]struct{}{
	"recording_completed": {}, "recording_interrupted": {}, "adapter_unavailable": {},
	"integrity_failure": {}, "export_failed": {}, "storage_threshold_exceeded": {},
}

// Tags returns the normalized local tag set for a recording.
func (s *Store) Tags(recordingID string) ([]string, error) {
	if err := validRecordingID(recordingID); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.forgetting[recordingID] {
		return []string{}, nil
	}
	return append([]string{}, s.tags[recordingID]...), nil
}

// SetTags atomically replaces a recording's management tags. An empty list
// clears the projection without changing its canonical archive.
func (s *Store) SetTags(recordingID string, tags []string) error {
	if err := validRecordingID(recordingID); err != nil {
		return err
	}
	normalized, err := normalizeTags(tags)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.forgetting[recordingID] {
		return fmt.Errorf("recording management projections are being removed")
	}
	path := s.recordingPath(recordingID)
	if err := s.write(path, tagsDocument{Tags: normalized}); err != nil {
		return fmt.Errorf("save recording tags: %w", err)
	}
	s.tags[recordingID] = append([]string{}, normalized...)
	return nil
}

// ForgetRecording removes only this recording's product-management
// projections. A durable marker is written first, and each projection file is
// atomically renamed to a tombstone before removal. If any step fails, the
// marker and in-process write fence remain so a retry or next Open can finish
// deletion without restoring stale projection state.
func (s *Store) ForgetRecording(recordingID string) error {
	if err := validRecordingID(recordingID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	markerPath := filepath.Join(s.root, "deletions", recordingID+".json")
	if err := s.write(markerPath, deletionMarker{RecordingID: recordingID}); err != nil {
		return fmt.Errorf("could not persist recording projection removal")
	}
	s.forgetting[recordingID] = true
	if err := s.removeRecordingProjections(recordingID); err != nil {
		return fmt.Errorf("recording projections could not be fully removed")
	}
	deletionDir := filepath.Join(s.root, "deletions")
	if err := os.Remove(markerPath); err != nil {
		return fmt.Errorf("recording projection removal is pending")
	}
	if err := syncDirectory(deletionDir); err != nil {
		return fmt.Errorf("recording projection removal is pending")
	}
	delete(s.tags, recordingID)
	delete(s.recordingEvents, recordingID)
	delete(s.forgetting, recordingID)
	return nil
}

func normalizeTags(tags []string) ([]string, error) {
	// Bound work even when this package is called without an HTTP body limit.
	if len(tags) > 256 {
		return nil, fmt.Errorf("too many tag entries")
	}
	set := make(map[string]string, len(tags))
	for _, tag := range tags {
		if len(tag) > 256 {
			return nil, fmt.Errorf("tag exceeds 64 characters")
		}
		value := strings.ToLower(strings.TrimSpace(tag))
		if value == "" || !utf8.ValidString(value) || strings.Contains(value, "://") {
			return nil, fmt.Errorf("tag must not be empty")
		}
		if utf8RuneCount(value) > 64 {
			return nil, fmt.Errorf("tag exceeds 64 characters")
		}
		for _, r := range value {
			if unicode.IsControl(r) {
				return nil, fmt.Errorf("tag contains a control character")
			}
		}
		folded := unicodeFoldKey(value)
		if previous, exists := set[folded]; !exists || value < previous {
			set[folded] = value
		}
	}
	if len(set) > 32 {
		return nil, fmt.Errorf("too many tags")
	}
	total := 0
	for _, value := range set {
		total += len(value)
	}
	if total > 2048 {
		return nil, fmt.Errorf("tags exceed total size limit")
	}
	result := make([]string, 0, len(set))
	for _, tag := range set {
		result = append(result, tag)
	}
	sort.Strings(result)
	return result, nil
}

func unicodeFoldKey(value string) string {
	var result strings.Builder
	for _, current := range value {
		minimum := current
		for next := unicode.SimpleFold(current); next != current; next = unicode.SimpleFold(next) {
			if next < minimum {
				minimum = next
			}
		}
		result.WriteRune(minimum)
	}
	return result.String()
}

func utf8RuneCount(value string) int {
	return len([]rune(value))
}

func (s *Store) AppendWorkflowHistory(event WorkflowHistoryEvent) error {
	if err := validateWorkflowHistoryEvent(event); err != nil {
		return err
	}
	event = cloneWorkflowEvent(event)
	s.mu.Lock()
	defer s.mu.Unlock()
	if containsWorkflowEventID(s.workflowHistory, event.ID) {
		return fmt.Errorf("duplicate workflow history event identifier")
	}
	next := append(append([]WorkflowHistoryEvent(nil), s.workflowHistory...), event)
	next = sortWorkflowHistory(next)
	if len(next) > maxWorkflowHistory {
		next = next[:maxWorkflowHistory]
	}
	if err := s.write(filepath.Join(s.root, "workflow-history.json"), next); err != nil {
		return fmt.Errorf("save workflow history: %w", err)
	}
	s.workflowHistory = next
	return nil
}

// WorkflowHistory returns matching history newest-first. A non-positive limit
// uses the package default and larger values are capped.
func (s *Store) WorkflowHistory(workflowID, adapterID, state string, limit int) []WorkflowHistoryEvent {
	limit = limitFor(limit, maxWorkflowHistory)
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]WorkflowHistoryEvent, 0)
	for _, event := range s.workflowHistory {
		if workflowID != "" && event.WorkflowID != workflowID || adapterID != "" && event.AdapterID != adapterID || state != "" && event.State != state {
			continue
		}
		result = append(result, cloneWorkflowEvent(event))
		if len(result) == limit {
			break
		}
	}
	return result
}

func (s *Store) AppendRecordingEvent(event RecordingEvent) error {
	if err := validateRecordingEvent(event); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.forgetting[event.RecordingID] {
		return fmt.Errorf("recording management projections are being removed")
	}
	current := s.recordingEvents[event.RecordingID]
	if containsRecordingEventID(current, event.ID) {
		return fmt.Errorf("duplicate recording event identifier")
	}
	next := append(append([]RecordingEvent(nil), current...), event)
	next = sortRecordingEvents(next)
	if len(next) > maxRecordingEvents {
		next = next[:maxRecordingEvents]
	}
	if err := s.write(s.recordingEventsPath(event.RecordingID), next); err != nil {
		return fmt.Errorf("save recording events: %w", err)
	}
	s.recordingEvents[event.RecordingID] = next
	return nil
}

// RecordingEvents returns a recording's safe timeline events newest-first.
func (s *Store) RecordingEvents(recordingID string, limit int) ([]RecordingEvent, error) {
	if err := validRecordingID(recordingID); err != nil {
		return nil, err
	}
	limit = limitFor(limit, maxRecordingEvents)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.forgetting[recordingID] {
		return []RecordingEvent{}, nil
	}
	items := s.recordingEvents[recordingID]
	if len(items) > limit {
		items = items[:limit]
	}
	return append([]RecordingEvent(nil), items...), nil
}

func (s *Store) AppendAudit(event AuditEvent) error {
	if event.Actor == nil {
		// Existing internal callers represent background/system actions. HTTP
		// mutations set a user actor explicitly from the authenticated principal.
		event.Actor = &AuditActor{Type: AuditActorSystem}
	}
	if err := validateAuditEvent(event); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if containsAuditID(s.audit, event.ID) {
		return fmt.Errorf("duplicate audit event identifier")
	}
	next := append(append([]AuditEvent(nil), s.audit...), event)
	next = sortAudit(next)
	if len(next) > maxAuditEvents {
		next = next[:maxAuditEvents]
	}
	if err := s.write(filepath.Join(s.root, "audit.json"), next); err != nil {
		return fmt.Errorf("save audit events: %w", err)
	}
	s.audit = next
	return nil
}

// AuditPage returns one stable newest-first page. Cursor identifies last event
// from previous page by timestamp and ID, so equal timestamps cannot duplicate
// or skip events.
func (s *Store) AuditPage(limit int, cursor string) ([]AuditEvent, string, error) {
	if limit <= 0 {
		limit = defaultQueryLimit
	}
	if limit > maxAuditPageSize {
		limit = maxAuditPageSize
	}
	var boundary auditCursor
	if cursor != "" {
		decoded, err := decodeAuditCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		boundary = decoded
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	start := 0
	if cursor != "" {
		start = sort.Search(len(s.audit), func(index int) bool {
			event := s.audit[index]
			return event.At.Before(boundary.At) || (event.At.Equal(boundary.At) && event.ID < boundary.ID)
		})
	}
	end := start + limit
	if end > len(s.audit) {
		end = len(s.audit)
	}
	items := append([]AuditEvent{}, s.audit[start:end]...)
	next := ""
	if end < len(s.audit) && len(items) != 0 {
		encoded, err := encodeAuditCursor(items[len(items)-1])
		if err != nil {
			return nil, "", err
		}
		next = encoded
	}
	return items, next, nil
}

func encodeAuditCursor(event AuditEvent) (string, error) {
	data, err := json.Marshal(auditCursor{Version: auditCursorVersion, At: event.At.UTC(), ID: event.ID})
	if err != nil || len(data) > maxAuditCursorBytes {
		return "", ErrInvalidAuditCursor
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeAuditCursor(encoded string) (auditCursor, error) {
	if len(encoded) == 0 || len(encoded) > maxAuditCursorBytes {
		return auditCursor{}, ErrInvalidAuditCursor
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(data) > maxAuditCursorBytes {
		return auditCursor{}, ErrInvalidAuditCursor
	}
	var cursor auditCursor
	if json.Unmarshal(data, &cursor) != nil || cursor.Version != auditCursorVersion || cursor.At.IsZero() || !validIdentifier(cursor.ID, 128) {
		return auditCursor{}, ErrInvalidAuditCursor
	}
	return cursor, nil
}

// Audit returns audit events newest-first.
func (s *Store) Audit(limit int) []AuditEvent {
	limit = limitFor(limit, maxAuditEvents)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.audit) > limit {
		return append([]AuditEvent(nil), s.audit[:limit]...)
	}
	return append([]AuditEvent(nil), s.audit...)
}

func (s *Store) AddNotification(notification Notification) error {
	if err := validateNotification(notification); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if containsNotificationID(s.notifications, notification.ID) {
		return ErrDuplicateNotification
	}
	next := append(append([]Notification(nil), s.notifications...), notification)
	next = sortNotifications(next)
	if len(next) > maxNotifications {
		next = next[:maxNotifications]
	}
	if err := s.write(filepath.Join(s.root, "notifications.json"), next); err != nil {
		return fmt.Errorf("save notifications: %w", err)
	}
	s.notifications = next
	return nil
}

// Notifications returns newest-first notifications, optionally only unread.
func (s *Store) Notifications(unreadOnly bool, limit int) []Notification {
	limit = limitFor(limit, maxNotifications)
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Notification, 0)
	for _, item := range s.notifications {
		if unreadOnly && item.Read {
			continue
		}
		result = append(result, item)
		if len(result) == limit {
			break
		}
	}
	return result
}

func (s *Store) MarkNotificationRead(id string) error {
	if !validIdentifier(id, 128) {
		return fmt.Errorf("invalid notification identifier")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next := append([]Notification(nil), s.notifications...)
	found := false
	for i := range next {
		if next[i].ID == id {
			next[i].Read = true
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("notification not found")
	}
	if err := s.write(filepath.Join(s.root, "notifications.json"), next); err != nil {
		return fmt.Errorf("save notification state: %w", err)
	}
	s.notifications = next
	return nil
}

func (s *Store) MarkAllNotificationsRead() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := append([]Notification(nil), s.notifications...)
	for i := range next {
		next[i].Read = true
	}
	if err := s.write(filepath.Join(s.root, "notifications.json"), next); err != nil {
		return fmt.Errorf("save notification state: %w", err)
	}
	s.notifications = next
	return nil
}

func (s *Store) recordingPath(id string) string {
	return filepath.Join(s.root, "recordings", id+".json")
}

func (s *Store) recordingEventsPath(id string) string {
	return filepath.Join(s.root, "recording-events", id+".json")
}

func validateWorkflowHistoryEvent(event WorkflowHistoryEvent) error {
	if !validIdentifier(event.ID, 128) || !validIdentifier(event.WorkflowID, 128) || !validIdentifier(event.AdapterID, 128) {
		return fmt.Errorf("invalid workflow history identifier")
	}
	if _, ok := workflowStates[event.State]; !ok {
		return fmt.Errorf("invalid workflow history state")
	}
	if event.At.IsZero() {
		return fmt.Errorf("workflow history timestamp is required")
	}
	if event.Resource != nil {
		if err := validateResourceRef(event.Resource); err != nil {
			return err
		}
	}
	if event.Challenge != nil {
		if !safeText(event.Challenge.Title, 256) || !safeText(event.Challenge.Message, 1024) || event.Challenge.FieldCount < 0 || event.Challenge.FieldCount > 256 {
			return fmt.Errorf("invalid workflow challenge summary")
		}
	}
	return nil
}

func validateResourceRef(ref *ResourceRef) error {
	seen := map[*ResourceRef]struct{}{}
	for depth, current := 0, ref; current != nil; depth, current = depth+1, current.Parent {
		if depth >= 32 {
			return fmt.Errorf("resource reference is too deep")
		}
		if _, exists := seen[current]; exists {
			return fmt.Errorf("resource reference contains a cycle")
		}
		seen[current] = struct{}{}
		if !safeText(current.Type, 128) || strings.TrimSpace(current.Type) == "" || !safeText(current.ID, 256) || strings.TrimSpace(current.ID) == "" {
			return fmt.Errorf("invalid resource reference")
		}
	}
	return nil
}

func validateRecordingEvent(event RecordingEvent) error {
	if !validIdentifier(event.ID, 128) {
		return fmt.Errorf("invalid recording event identifier")
	}
	if err := validRecordingID(event.RecordingID); err != nil {
		return err
	}
	if _, ok := recordingEventTypes[event.Type]; !ok {
		return fmt.Errorf("invalid recording event type")
	}
	if event.At.IsZero() || event.Count < 0 || event.Count > 1<<31 {
		return fmt.Errorf("invalid recording event data")
	}
	if _, ok := safeRecordingMessages[event.Message]; event.Message != "" && !ok {
		return fmt.Errorf("recording event message is not an approved summary")
	}
	return nil
}

func validateAuditEvent(event AuditEvent) error {
	if !validIdentifier(event.ID, 128) || event.At.IsZero() {
		return fmt.Errorf("invalid audit event")
	}
	if _, ok := auditTypes[event.Type]; !ok {
		return fmt.Errorf("invalid audit event type")
	}
	if event.ObjectID != "" && !validIdentifier(event.ObjectID, 256) {
		return fmt.Errorf("invalid audit object identifier")
	}
	if event.Actor == nil {
		return nil // Legacy audit entries had no actor.
	}
	switch event.Actor.Type {
	case "":
		return fmt.Errorf("invalid audit actor type")
	case AuditActorSystem:
		if event.Actor.UserID != "" {
			return fmt.Errorf("invalid system audit actor")
		}
	case AuditActorUser:
		if !validIdentifier(event.Actor.UserID, 128) {
			return fmt.Errorf("invalid user audit actor")
		}
	default:
		return fmt.Errorf("invalid audit actor type")
	}
	return nil
}

func validateNotification(notification Notification) error {
	if !validIdentifier(notification.ID, 128) || notification.At.IsZero() {
		return fmt.Errorf("invalid notification")
	}
	if _, ok := notificationTypes[notification.Type]; !ok {
		return fmt.Errorf("invalid notification type")
	}
	if notification.ObjectID != "" && !validIdentifier(notification.ObjectID, 256) {
		return fmt.Errorf("invalid notification object identifier")
	}
	return nil
}

func safeText(value string, maxRunes int) bool {
	if len(value) > maxRunes*4 || !utf8.ValidString(value) || len([]rune(value)) > maxRunes || strings.Contains(value, "://") {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func cloneWorkflowEvent(event WorkflowHistoryEvent) WorkflowHistoryEvent {
	if event.Resource != nil {
		event.Resource = cloneResource(event.Resource)
	}
	if event.Challenge != nil {
		challenge := *event.Challenge
		event.Challenge = &challenge
	}
	return event
}

func cloneResource(ref *ResourceRef) *ResourceRef {
	if ref == nil {
		return nil
	}
	copyRef := &ResourceRef{Type: ref.Type, ID: ref.ID}
	copyRef.Parent = cloneResource(ref.Parent)
	return copyRef
}

func containsWorkflowEventID(events []WorkflowHistoryEvent, id string) bool {
	for _, event := range events {
		if event.ID == id {
			return true
		}
	}
	return false
}

func containsRecordingEventID(events []RecordingEvent, id string) bool {
	for _, event := range events {
		if event.ID == id {
			return true
		}
	}
	return false
}

func containsAuditID(events []AuditEvent, id string) bool {
	for _, event := range events {
		if event.ID == id {
			return true
		}
	}
	return false
}

func containsNotificationID(events []Notification, id string) bool {
	for _, event := range events {
		if event.ID == id {
			return true
		}
	}
	return false
}

func hasDuplicateWorkflowEventIDs(events []WorkflowHistoryEvent) bool {
	seen := make(map[string]struct{}, len(events))
	for _, event := range events {
		if _, exists := seen[event.ID]; exists {
			return true
		}
		seen[event.ID] = struct{}{}
	}
	return false
}

func hasDuplicateRecordingEventIDs(events []RecordingEvent) bool {
	seen := make(map[string]struct{}, len(events))
	for _, event := range events {
		if _, exists := seen[event.ID]; exists {
			return true
		}
		seen[event.ID] = struct{}{}
	}
	return false
}

func hasDuplicateAuditIDs(events []AuditEvent) bool {
	seen := make(map[string]struct{}, len(events))
	for _, event := range events {
		if _, exists := seen[event.ID]; exists {
			return true
		}
		seen[event.ID] = struct{}{}
	}
	return false
}

func hasDuplicateNotificationIDs(events []Notification) bool {
	seen := make(map[string]struct{}, len(events))
	for _, event := range events {
		if _, exists := seen[event.ID]; exists {
			return true
		}
		seen[event.ID] = struct{}{}
	}
	return false
}

func sortWorkflowHistory(events []WorkflowHistoryEvent) []WorkflowHistoryEvent {
	items := append([]WorkflowHistoryEvent(nil), events...)
	sortByTime(items)
	return items
}

func sortRecordingEvents(events []RecordingEvent) []RecordingEvent {
	items := append([]RecordingEvent(nil), events...)
	sortByTime(items)
	return items
}

func sortAudit(events []AuditEvent) []AuditEvent {
	items := append([]AuditEvent(nil), events...)
	sortByTime(items)
	return items
}

func sortNotifications(events []Notification) []Notification {
	items := append([]Notification(nil), events...)
	sortByTime(items)
	return items
}

func (event WorkflowHistoryEvent) eventTime() time.Time { return event.At }
func (event WorkflowHistoryEvent) eventID() string      { return event.ID }
func (event RecordingEvent) eventTime() time.Time       { return event.At }
func (event RecordingEvent) eventID() string            { return event.ID }
func (event AuditEvent) eventTime() time.Time           { return event.At }
func (event AuditEvent) eventID() string                { return event.ID }
func (event Notification) eventTime() time.Time         { return event.At }
func (event Notification) eventID() string              { return event.ID }
