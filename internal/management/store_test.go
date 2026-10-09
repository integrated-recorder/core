package management

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/storagediagnostic"
)

const testRecordingID = "0123456789abcdef0123456789abcdef"

func TestForgetRecordingRemovesPrivateStorageDiagnostic(t *testing.T) {
	store, root := openTestStore(t)
	diagnostics, err := storagediagnostic.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := storagediagnostic.Diagnostic{
		Version: storagediagnostic.SchemaVersion, RecordedAt: time.Now().UTC(), RecordingID: testRecordingID,
		Classification: storagediagnostic.ClassificationCanonicalCommitFailed,
		StageChain:     []string{"recording root commit"}, ErrorTypeChain: []string{"*os.PathError"},
		FileOperation: "rename", ErrnoCode: 5,
		IngestSnapshot: storagediagnostic.IngestSnapshot{},
	}
	if _, _, err := diagnostics.RecordFirst(diagnostic); err != nil {
		t.Fatal(err)
	}
	if err := store.SetTags(testRecordingID, []string{"temporary"}); err != nil {
		t.Fatal(err)
	}
	if err := store.ForgetRecording(testRecordingID); err != nil {
		t.Fatal(err)
	}
	if _, err := diagnostics.Read(testRecordingID); err != storagediagnostic.ErrNotFound {
		t.Fatalf("diagnostic after recording deletion: %v", err)
	}
}

func openTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	return store, root
}

func TestTagsNormalizeRoundTripAndReload(t *testing.T) {
	store, root := openTestStore(t)
	if err := store.SetTags(testRecordingID, []string{" Concert ", "important", "CONCERT", "Archive", "Σ", "ς"}); err != nil {
		t.Fatal(err)
	}
	want := []string{"archive", "concert", "important", "ς"}
	got, err := store.Tags(testRecordingID)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("Tags() = %v, %v; want %v", got, err, want)
	}
	reloaded, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	got, err = reloaded.Tags(testRecordingID)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("reloaded Tags() = %v, %v; want %v", got, err, want)
	}
	if err := reloaded.SetTags(testRecordingID, nil); err != nil {
		t.Fatal(err)
	}
	got, err = reloaded.Tags(testRecordingID)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty tag replacement = %v, %v", got, err)
	}
}

func TestTagLimitsAndValidation(t *testing.T) {
	store, _ := openTestStore(t)
	tests := []struct {
		name string
		tags []string
	}{
		{name: "empty", tags: []string{" "}},
		{name: "control", tags: []string{"unsafe\nvalue"}},
		{name: "too many", tags: numberedTags(33, 1)},
		{name: "too long", tags: []string{strings.Repeat("x", 65)}},
		{name: "total bytes", tags: repeatedUTF8Tags(32, strings.Repeat("界", 64))},
		{name: "excessive duplicate input", tags: make([]string, 257)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := store.SetTags(testRecordingID, tc.tags); err == nil {
				t.Fatal("SetTags() unexpectedly accepted invalid tags")
			}
		})
	}
	if _, err := store.Tags("../recording"); err == nil {
		t.Fatal("Tags() accepted a path-like recording ID")
	}
}

func TestTagWriteFailureDoesNotChangeMemory(t *testing.T) {
	store, _ := openTestStore(t)
	if err := store.SetTags(testRecordingID, []string{"old"}); err != nil {
		t.Fatal(err)
	}
	path := store.recordingPath(testRecordingID)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.SetTags(testRecordingID, []string{"new"}); err == nil {
		t.Fatal("SetTags() unexpectedly succeeded with a directory at target path")
	}
	got, err := store.Tags(testRecordingID)
	if err != nil || !reflect.DeepEqual(got, []string{"old"}) {
		t.Fatalf("failed write changed in-memory tags: %v, %v", got, err)
	}
}

func TestWorkflowHistoryOrderLimitAndAnswerFreeShape(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	// Seed the bounded collection directly to keep this boundary test fast.
	seed := make([]WorkflowHistoryEvent, maxWorkflowHistory)
	for i := range seed {
		seed[i] = WorkflowHistoryEvent{
			ID:         strings.Repeat("a", 8) + strings.Repeat("0", 4) + pad4(i),
			WorkflowID: "workflow",
			AdapterID:  "opaque.adapter",
			State:      "continued",
			At:         base.Add(time.Duration(i) * time.Second),
		}
	}
	if err := store.write(filepath.Join(store.root, "workflow-history.json"), seed); err != nil {
		t.Fatal(err)
	}
	store, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendWorkflowHistory(WorkflowHistoryEvent{
		ID:         "event-latest",
		WorkflowID: "workflow",
		AdapterID:  "opaque.adapter",
		State:      "challenge_required",
		At:         base.Add(time.Duration(maxWorkflowHistory) * time.Second),
		Resource:   &ResourceRef{Type: "opaque-type", ID: "opaque-resource"},
		Challenge:  &ChallengeSummary{Title: "Input required", FieldCount: 2, HasSecretFields: true},
	}); err != nil {
		t.Fatal(err)
	}
	got := store.WorkflowHistory("workflow", "opaque.adapter", "", 2)
	if len(got) != 2 || got[0].ID != "event-latest" || got[1].ID != seed[maxWorkflowHistory-1].ID {
		t.Fatalf("workflow history order/limit = %+v", got)
	}
	encoded, err := json.Marshal(got[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"answer", "secret_value", "password", "value"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("history contains forbidden answer field %q: %s", forbidden, encoded)
		}
	}
	if got[0].Challenge == nil || !got[0].Challenge.HasSecretFields {
		t.Fatal("safe secret-field summary was not retained")
	}
}

func TestRecordingEventsWhitelistAndBound(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	seed := make([]RecordingEvent, maxRecordingEvents)
	for i := range seed {
		seed[i] = RecordingEvent{ID: "recording-event-" + pad4(i), RecordingID: testRecordingID, Type: "manifest_observed", At: base.Add(time.Duration(i) * time.Second), Count: i}
	}
	if err := store.write(store.recordingEventsPath(testRecordingID), seed); err != nil {
		t.Fatal(err)
	}
	store, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendRecordingEvent(RecordingEvent{ID: "newest-event", RecordingID: testRecordingID, Type: "gap_detected", At: base.Add(time.Duration(maxRecordingEvents) * time.Second), Count: 2, Message: "gap detected"}); err != nil {
		t.Fatal(err)
	}
	got, err := store.RecordingEvents(testRecordingID, 2)
	if err != nil || len(got) != 2 || got[0].ID != "newest-event" {
		t.Fatalf("RecordingEvents() = %+v, %v", got, err)
	}
	if _, err := store.RecordingEvents("../outside", 10); err == nil {
		t.Fatal("RecordingEvents() accepted path-like ID")
	}
	if err := store.AppendRecordingEvent(RecordingEvent{ID: "bad-event", RecordingID: testRecordingID, Type: "gap_detected", At: base, Message: "https://example.invalid/?token=secret"}); err == nil {
		t.Fatal("recording event accepted arbitrary URL/message")
	}
}

func TestAuditAndNotificationSemantics(t *testing.T) {
	store, root := openTestStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	for _, event := range []AuditEvent{
		{ID: "audit-1", Type: "recording_deleted", At: now, ObjectID: testRecordingID},
		{ID: "audit-2", Type: "adapter_restarted", At: now.Add(time.Second), ObjectID: "opaque.adapter"},
		{ID: "audit-3", Type: "recording_tags_updated", At: now.Add(2 * time.Second), ObjectID: testRecordingID},
	} {
		if err := store.AppendAudit(event); err != nil {
			t.Fatal(err)
		}
	}
	if got := store.Audit(1); len(got) != 1 || got[0].ID != "audit-3" {
		t.Fatalf("Audit(1) = %+v", got)
	}
	if err := store.AppendAudit(AuditEvent{ID: "bad", Type: "config_changed", At: now, ObjectID: "token=secret"}); err == nil {
		t.Fatal("audit accepted arbitrary object identifier")
	}
	if got := store.Audit(10)[0].Actor; got == nil || got.Type != AuditActorSystem {
		t.Fatalf("implicit system audit actor = %+v", got)
	}
	for _, notification := range []Notification{
		{ID: "notice-1", Type: "recording_completed", At: now, ObjectID: testRecordingID},
		{ID: "notice-2", Type: "integrity_failure", At: now.Add(time.Second), ObjectID: testRecordingID},
	} {
		if err := store.AddNotification(notification); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.MarkNotificationRead("notice-1"); err != nil {
		t.Fatal(err)
	}
	got := store.Notifications(true, 10)
	if len(got) != 1 || got[0].ID != "notice-2" {
		t.Fatalf("unread notifications = %+v", got)
	}
	if err := store.MarkAllNotificationsRead(); err != nil {
		t.Fatal(err)
	}
	if got := store.Notifications(true, 10); len(got) != 0 {
		t.Fatalf("notifications remain unread: %+v", got)
	}
	reloaded, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.Notifications(false, 10); len(got) != 2 || !got[0].Read || !got[1].Read {
		t.Fatalf("notification read state did not persist: %+v", got)
	}
}

func TestAuditActorValidationAndCursorPaginationAcrossTenThousandEvents(t *testing.T) {
	store, root := openTestStore(t)
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	events := make([]AuditEvent, maxAuditEvents)
	for i := range events {
		events[i] = AuditEvent{
			ID: fmt.Sprintf("audit-%05d", i), Type: "config_changed",
			At:    base.Add(time.Duration(i%23) * time.Second),
			Actor: &AuditActor{Type: AuditActorUser, UserID: "user_0123456789abcdef0123456789abcdef"},
		}
	}
	store.audit = sortAudit(events)
	if err := store.write(filepath.Join(store.root, "audit.json"), store.audit); err != nil {
		t.Fatal(err)
	}

	// Reload persisted data. This checks actor schema and pagination over the
	// actual 10,000-event store limit without 10,000 full-file rewrites.
	store, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	want := store.Audit(maxAuditEvents)
	seen := make(map[string]bool, len(want))
	var gotAll []AuditEvent
	cursor := ""
	for pageNumber := 0; ; pageNumber++ {
		page, next, err := store.AuditPage(137, cursor)
		if err != nil {
			t.Fatalf("AuditPage(%d): %v", pageNumber, err)
		}
		if len(page) == 0 && cursor != "" {
			break
		}
		for _, event := range page {
			if seen[event.ID] {
				t.Fatalf("duplicate audit event %q", event.ID)
			}
			seen[event.ID] = true
			gotAll = append(gotAll, event)
		}
		if len(page) > 137 {
			t.Fatalf("page size %d exceeded requested bound", len(page))
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if len(gotAll) != len(want) {
		t.Fatalf("paginated %d audit events, want %d", len(gotAll), len(want))
	}
	for i := range want {
		if gotAll[i].ID != want[i].ID {
			t.Fatalf("page order[%d]=%q, want %q", i, gotAll[i].ID, want[i].ID)
		}
		if gotAll[i].Actor == nil || gotAll[i].Actor.Type != AuditActorUser || gotAll[i].Actor.UserID != "user_0123456789abcdef0123456789abcdef" {
			t.Fatalf("actor[%d]=%+v", i, gotAll[i].Actor)
		}
	}

	if _, _, err := store.AuditPage(10, strings.Repeat("x", maxAuditCursorBytes+1)); !errors.Is(err, ErrInvalidAuditCursor) {
		t.Fatalf("oversized cursor error=%v", err)
	}
	if err := store.AppendAudit(AuditEvent{ID: "bad-actor", Type: "config_changed", At: base, Actor: &AuditActor{Type: AuditActorUser, UserID: "../admin"}}); err == nil {
		t.Fatal("audit accepted path-like user ID")
	}
}

func TestWatchAuditActionsAreExplicitlyAllowlisted(t *testing.T) {
	store, _ := openTestStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	actions := []string{"watch_created", "watch_updated", "watch_enabled", "watch_disabled", "watch_deleted", "manual_watch_check"}
	for i, action := range actions {
		if err := store.AppendAudit(AuditEvent{
			ID: fmt.Sprintf("watch-audit-%d", i), Type: action, At: now.Add(time.Duration(i) * time.Second), ObjectID: "watch-0123456789abcdef0123456789abcdef",
		}); err != nil {
			t.Fatalf("watch audit action %q rejected: %v", action, err)
		}
	}
	if err := store.AppendAudit(AuditEvent{ID: "unknown-watch-audit", Type: "watch_secret_read", At: now.Add(time.Minute), ObjectID: "watch-0123456789abcdef0123456789abcdef"}); err == nil {
		t.Fatal("unknown watch_* audit action was accepted")
	}
}

func TestForgetRecordingRemovesOnlyManagementProjectionsAndIsIdempotent(t *testing.T) {
	store, root := openTestStore(t)
	if err := store.SetTags(testRecordingID, []string{"keep? no", "archive"}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendRecordingEvent(RecordingEvent{ID: "event-forget", RecordingID: testRecordingID, Type: "recording_started", At: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	canonicalDir := filepath.Join(root, "recordings", testRecordingID)
	if err := os.MkdirAll(canonicalDir, 0700); err != nil {
		t.Fatal(err)
	}
	canonical := filepath.Join(canonicalDir, "recording.json")
	if err := os.WriteFile(canonical, []byte("{\"id\":\"canonical\"}"), 0600); err != nil {
		t.Fatal(err)
	}

	if err := store.ForgetRecording(testRecordingID); err != nil {
		t.Fatal(err)
	}
	if err := store.ForgetRecording(testRecordingID); err != nil {
		t.Fatalf("second ForgetRecording() was not idempotent: %v", err)
	}
	if tags, err := store.Tags(testRecordingID); err != nil || len(tags) != 0 {
		t.Fatalf("tags remain after forgetting: %v, %v", tags, err)
	}
	if events, err := store.RecordingEvents(testRecordingID, 10); err != nil || len(events) != 0 {
		t.Fatalf("events remain after forgetting: %v, %v", events, err)
	}
	for _, path := range []string{store.recordingPath(testRecordingID), store.recordingEventsPath(testRecordingID)} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("projection still exists at internal location: %v", err)
		}
	}
	got, err := os.ReadFile(canonical)
	if err != nil || string(got) != "{\"id\":\"canonical\"}" {
		t.Fatalf("canonical archive was modified: %q, %v", got, err)
	}
}

func TestForgetRecordingPartialFailureIsRetryableAndFencesWrites(t *testing.T) {
	store, _ := openTestStore(t)
	if err := store.SetTags(testRecordingID, []string{"archive"}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendRecordingEvent(RecordingEvent{ID: "event-before-forget", RecordingID: testRecordingID, Type: "recording_started", At: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	eventPath := store.recordingEventsPath(testRecordingID)
	if err := os.Remove(eventPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(eventPath, 0700); err != nil {
		t.Fatal(err)
	}

	if err := store.ForgetRecording(testRecordingID); err == nil {
		t.Fatal("ForgetRecording() unexpectedly succeeded with an unsafe event path")
	}
	if tags, err := store.Tags(testRecordingID); err != nil || len(tags) != 0 {
		t.Fatalf("partially forgotten tags were exposed: %v, %v", tags, err)
	}
	if err := store.SetTags(testRecordingID, []string{"stale"}); err == nil {
		t.Fatal("SetTags() wrote during pending removal")
	}
	if err := store.AppendRecordingEvent(RecordingEvent{ID: "event-stale", RecordingID: testRecordingID, Type: "recording_started", At: time.Now().UTC()}); err == nil {
		t.Fatal("AppendRecordingEvent() wrote during pending removal")
	}
	if _, err := os.Stat(filepath.Join(store.root, "deletions", testRecordingID+".json")); err != nil {
		t.Fatalf("retry marker was not retained: %v", err)
	}

	if err := os.Remove(eventPath); err != nil {
		t.Fatal(err)
	}
	if err := store.ForgetRecording(testRecordingID); err != nil {
		t.Fatalf("retry did not finish removal: %v", err)
	}
	if _, err := os.Stat(filepath.Join(store.root, "deletions", testRecordingID+".json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deletion marker remains after success: %v", err)
	}
}

func TestOpenRecoversPendingRecordingForget(t *testing.T) {
	store, root := openTestStore(t)
	if err := store.SetTags(testRecordingID, []string{"archive"}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendRecordingEvent(RecordingEvent{ID: "event-recovery", RecordingID: testRecordingID, Type: "recording_started", At: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	eventPath := store.recordingEventsPath(testRecordingID)
	if err := os.Remove(eventPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(eventPath, 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.ForgetRecording(testRecordingID); err == nil {
		t.Fatal("expected partial deletion")
	}
	if err := os.Remove(eventPath); err != nil {
		t.Fatal(err)
	}

	reloaded, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if tags, err := reloaded.Tags(testRecordingID); err != nil || len(tags) != 0 {
		t.Fatalf("pending tags were restored: %v, %v", tags, err)
	}
	if events, err := reloaded.RecordingEvents(testRecordingID, 10); err != nil || len(events) != 0 {
		t.Fatalf("pending events were restored: %v, %v", events, err)
	}
	if _, err := os.Stat(filepath.Join(reloaded.root, "deletions", testRecordingID+".json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pending marker was not cleaned: %v", err)
	}
}

func TestManagementPermissions(t *testing.T) {
	store, root := openTestStore(t)
	if err := store.SetTags(testRecordingID, []string{"safe"}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendAudit(AuditEvent{ID: "audit-permissions", Type: "recording_deleted", At: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{
		filepath.Join(root, "management"),
		filepath.Join(root, "management", "recordings"),
		filepath.Join(root, "management", "recording-events"),
	} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0700 {
			t.Errorf("directory %s mode = %04o, want 0700", filepath.Base(dir), got)
		}
	}
	for _, path := range []string{store.recordingPath(testRecordingID), filepath.Join(store.root, "audit.json")} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0600 {
			t.Errorf("file %s mode = %04o, want 0600", filepath.Base(path), got)
		}
	}
}

func TestOpenRejectsMalformedProjectionWithoutTouchingCanonical(t *testing.T) {
	root := t.TempDir()
	canonical := filepath.Join(root, "recordings", testRecordingID)
	if err := os.MkdirAll(canonical, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(canonical, "recording.json")
	if err := os.WriteFile(marker, []byte(`{"id":"canonical-marker"}`), 0600); err != nil {
		t.Fatal(err)
	}
	managementRoot := filepath.Join(root, "management")
	if err := os.MkdirAll(managementRoot, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(managementRoot, "audit.json"), []byte(`[{"id":"leak","type":"recording_deleted","at":"2025-01-01T00:00:00Z","secret":"answer"}]`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root); err == nil {
		t.Fatal("Open() accepted malformed projection")
	}
	got, err := os.ReadFile(marker)
	if err != nil || string(got) != `{"id":"canonical-marker"}` {
		t.Fatalf("canonical marker changed: %q, %v", got, err)
	}
}

func TestOpenCleansAtomicWriteResidue(t *testing.T) {
	root := t.TempDir()
	managementRoot := filepath.Join(root, "management")
	if err := os.MkdirAll(filepath.Join(managementRoot, "recordings"), 0700); err != nil {
		t.Fatal(err)
	}
	temp := filepath.Join(managementRoot, "recordings", ".projection-interrupted.tmp")
	if err := os.WriteFile(temp, []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(temp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary projection was not removed: %v", err)
	}
}

func TestMarkUnknownNotificationReturnsError(t *testing.T) {
	store, _ := openTestStore(t)
	if err := store.MarkNotificationRead("missing"); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("MarkNotificationRead() error = %v", err)
	}
}

func numberedTags(count, width int) []string {
	tags := make([]string, count)
	for i := range tags {
		tags[i] = strings.Repeat(string(rune('a'+i%26)), width) + pad4(i)
	}
	return tags
}

func repeatedUTF8Tags(count int, tag string) []string {
	tags := make([]string, count)
	for i := range tags {
		tags[i] = string(rune('a'+i)) + tag
	}
	return tags
}

func pad4(value int) string {
	return fmt.Sprintf("%04d", value)
}
