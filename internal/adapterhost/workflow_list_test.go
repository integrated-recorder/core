package adapterhost

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/interaction"
)

func TestListWorkflowsReturnsSafeOrderedSummaries(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	resource := &adapterproto.ResourceRef{
		Type: "opaque.beta",
		ID:   "opaque-child",
		Parent: &adapterproto.ResourceRef{
			Type: "opaque.alpha",
			ID:   "opaque-parent",
		},
	}
	challenge := &adapterproto.WorkflowChallenge{
		Schema: adapterproto.Schema{Fields: []adapterproto.Field{
			{Key: "ordinary", Control: "text", Label: "Ordinary", Default: json.RawMessage(`"default-secret-sentinel"`)},
			{Key: "opaque_secret", Control: "secret", Label: "Secret"},
		}},
		Prompt: &adapterproto.InteractionMessage{
			Type:          "secret_prompt",
			InteractionID: "opaque-interaction",
			Title:         "prompt-title-sentinel",
			Message:       "prompt-message-sentinel",
			Data:          json.RawMessage(`{"value":"prompt-data-sentinel"}`),
		},
	}
	host := &Host{workflows: map[string]workflowSession{
		"older": {
			adapterID: "opaque-adapter", workflowID: "older", resource: resource,
			createdAt: now.Add(-time.Minute), updatedAt: now.Add(-time.Second),
			progress: WorkflowProgress{State: "configuration_required", Challenge: challenge,
				Media: &adapterproto.MediaSource{Type: "hls", ManifestURL: "https://media.example/live.m3u8?token=media-sentinel", Headers: map[string]string{"Authorization": "header-sentinel"}}},
		},
		"newer": {
			adapterID: "opaque-adapter", workflowID: "newer", createdAt: now,
			updatedAt: now, continuing: true,
			progress: WorkflowProgress{State: "interaction_required"},
		},
	}}

	items := host.ListWorkflows()
	if len(items) != 2 {
		t.Fatalf("workflow summaries count = %d, want 2", len(items))
	}
	if items[0].WorkflowID != "newer" || !items[0].InProgress {
		t.Fatalf("newest/in-progress summary = %#v", items[0])
	}
	if items[1].WorkflowID != "older" || items[1].InProgress {
		t.Fatalf("older workflow summary = %#v", items[1])
	}
	wantExpiry := now.Add(-time.Second).Add(workflowTTL)
	if !items[1].ExpiresAt.Equal(wantExpiry) {
		t.Fatalf("expires_at = %s, want %s", items[1].ExpiresAt, wantExpiry)
	}
	if items[1].CreatedAt != now.Add(-time.Minute) || items[1].UpdatedAt != now.Add(-time.Second) {
		t.Fatalf("workflow timestamps changed: %#v", items[1])
	}
	if items[1].State != "configuration_required" || items[1].AdapterID != "opaque-adapter" {
		t.Fatalf("workflow identity/state missing: %#v", items[1])
	}
	if items[1].Resource == nil || items[1].Resource.Type != "opaque.beta" || items[1].Resource.ID != "opaque-child" || items[1].Resource.Parent == nil || items[1].Resource.Parent.Type != "opaque.alpha" {
		t.Fatalf("opaque resource chain not preserved: %#v", items[1].Resource)
	}
	if items[1].Challenge == nil || items[1].Challenge.FieldCount != 2 || !items[1].Challenge.HasSecretFields || items[1].Challenge.PromptType != "secret_prompt" {
		t.Fatalf("safe challenge summary = %#v", items[1].Challenge)
	}

	// The result must be an independent copy of the resource reference graph.
	items[1].Resource.Parent.ID = "mutated"
	again := host.ListWorkflows()
	if again[1].Resource.Parent.ID != "opaque-parent" {
		t.Fatalf("summary mutation changed host workflow resource: %#v", again[1].Resource)
	}

	encoded, err := json.Marshal(again)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"prompt-title-sentinel",
		"prompt-message-sentinel",
		"prompt-data-sentinel",
		"default-secret-sentinel",
		"media-sentinel",
		"header-sentinel",
		"Authorization",
	} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("workflow summary leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestListWorkflowsExpiresSessionsAndInteractions(t *testing.T) {
	tracker := interaction.NewTracker()
	prompt := adapterproto.InteractionMessage{Type: "prompt", InteractionID: "expired-list-prompt"}
	if _, err := tracker.Apply(prompt); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-workflowTTL - time.Second)
	host := &Host{
		interactions: tracker,
		workflows: map[string]workflowSession{
			"expired": {adapterID: "opaque", workflowID: "expired", createdAt: old, updatedAt: old, progress: WorkflowProgress{Challenge: &adapterproto.WorkflowChallenge{Prompt: &prompt}}},
			"active":  {adapterID: "opaque", workflowID: "active", createdAt: time.Now().UTC(), updatedAt: time.Now().UTC()},
		},
	}
	items := host.ListWorkflows()
	if len(items) != 1 || items[0].WorkflowID != "active" {
		t.Fatalf("list after lazy expiration = %#v", items)
	}
	if _, err := tracker.Get(prompt.InteractionID); err == nil {
		t.Fatal("expired workflow interaction was not removed")
	}
	if _, exists := host.workflows["expired"]; exists {
		t.Fatal("expired workflow remained in host map")
	}
	events := host.PendingWorkflowLifecycleEvents()
	if len(events) != 1 || events[0].State != "expired" || events[0].WorkflowID != "expired" {
		t.Fatalf("expiry lifecycle events = %#v", events)
	}
}

func TestWorkflowLifecycleTerminalEventsAreSafeAndAcknowledged(t *testing.T) {
	resource := &adapterproto.ResourceRef{Type: "opaque.alpha", ID: "resource-id"}
	challenge := &adapterproto.WorkflowChallenge{
		Schema: adapterproto.Schema{Fields: []adapterproto.Field{
			{Key: "secret", Control: "secret", Label: "Secret", Default: json.RawMessage(`"default-secret-sentinel"`)},
			{Key: "ordinary", Control: "text", Label: "Ordinary"},
		}},
		Prompt: &adapterproto.InteractionMessage{
			Type: "secret_prompt", InteractionID: "interaction-id",
			Title: "prompt-title-sentinel", Message: "prompt-message-sentinel",
			Fields: []adapterproto.InteractionField{{Key: "secret", Control: "secret", Label: "field-label-sentinel"}},
			Data:   json.RawMessage(`{"token":"prompt-data-sentinel"}`),
		},
	}
	now := time.Now().UTC()
	host := &Host{workflows: map[string]workflowSession{
		"terminal": {
			adapterID: "opaque-adapter", workflowID: "terminal", resource: resource,
			createdAt: now, updatedAt: now,
			progress: WorkflowProgress{WorkflowID: "terminal", AdapterID: "opaque-adapter", State: "configuration_required", Resource: resource, Challenge: challenge,
				Media: &adapterproto.MediaSource{Type: "hls", ManifestURL: "https://media.example/live.m3u8?token=media-sentinel"}},
		},
	}}
	descriptor := adapterproto.Descriptor{ID: "opaque-adapter", MediaTypes: []string{"hls"}, ResourceTypes: []adapterproto.ResourceType{{Type: "opaque.alpha"}}}
	progress, err := host.advanceWorkflow(context.Background(), descriptor, host.workflows["terminal"], adapterproto.ResolveWorkflowResult{
		State: "resolved", WorkflowID: "terminal", Resource: resource,
		Media: &adapterproto.MediaSource{Type: "hls", ManifestURL: "https://media.example/live.m3u8"},
	})
	if err != nil || progress.State != "resolved" {
		t.Fatalf("terminal workflow progress=%#v err=%v", progress, err)
	}
	first := host.PendingWorkflowLifecycleEvents()
	second := host.PendingWorkflowLifecycleEvents()
	if len(first) != 1 || len(second) != 1 || first[0].ID == "" || first[0].ID != second[0].ID {
		t.Fatalf("pending event IDs are not stable: first=%#v second=%#v", first, second)
	}
	event := first[0]
	if event.State != "resolved" || event.At.IsZero() || event.AdapterID != "opaque-adapter" || event.WorkflowID != "terminal" || event.Resource == nil || event.Resource.ID != "resource-id" {
		t.Fatalf("terminal event identity = %#v", event)
	}
	if event.Challenge == nil || event.Challenge.PromptType != "secret_prompt" || event.Challenge.FieldCount != 2 || !event.Challenge.HasSecretFields {
		t.Fatalf("terminal challenge projection = %#v", event.Challenge)
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"default-secret-sentinel", "prompt-title-sentinel", "prompt-message-sentinel", "field-label-sentinel", "prompt-data-sentinel", "media-sentinel", "https://media.example"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("terminal event leaked %q: %s", forbidden, encoded)
		}
	}
	if !host.AckWorkflowLifecycleEvent(event.ID) || host.AckWorkflowLifecycleEvent(event.ID) || len(host.PendingWorkflowLifecycleEvents()) != 0 {
		t.Fatal("lifecycle acknowledgement did not remove exactly one event")
	}
}

func TestWorkflowLifecycleCancellationAndFailureEvents(t *testing.T) {
	now := time.Now().UTC()
	challenge := &adapterproto.WorkflowChallenge{Schema: adapterproto.Schema{Fields: []adapterproto.Field{{Key: "secret", Control: "secret"}}}}
	host := &Host{workflows: map[string]workflowSession{
		"cancel": {adapterID: "opaque", workflowID: "cancel", createdAt: now, updatedAt: now, progress: WorkflowProgress{Challenge: challenge}},
		"failed": {adapterID: "opaque", workflowID: "failed", createdAt: now, updatedAt: now, progress: WorkflowProgress{Challenge: challenge}},
	}}
	if err := host.CancelWorkflow("cancel"); err != nil {
		t.Fatal(err)
	}
	_, err := host.advanceWorkflow(context.Background(), adapterproto.Descriptor{ID: "opaque"}, workflowSession{
		adapterID: "opaque", workflowID: "failed", createdAt: now, updatedAt: now,
		progress: WorkflowProgress{Challenge: challenge},
	}, adapterproto.ResolveWorkflowResult{State: "error", WorkflowID: "failed"})
	if !errors.Is(err, ErrWorkflowFailed) {
		t.Fatalf("explicit adapter error = %v", err)
	}
	events := host.PendingWorkflowLifecycleEvents()
	states := map[string]string{}
	for _, event := range events {
		states[event.WorkflowID] = event.State
	}
	if states["cancel"] != "canceled" || states["failed"] != "failed" {
		t.Fatalf("terminal lifecycle states = %#v", states)
	}
	if len(host.ListWorkflows()) != 0 {
		t.Fatalf("terminal workflows remain active: %#v", host.ListWorkflows())
	}
}

func TestContinuationAdapterErrorEmitsFailedTerminalEvent(t *testing.T) {
	dir := t.TempDir()
	writeAdapter(t, dir, binaryPrefix+"workflow-error", "workflow_error", "workflow-error", true)
	host, err := Discover(context.Background(), dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	progress, err := host.BeginResolution(context.Background(), "workflow-error", json.RawMessage(`{"manifest_url":"https://media.example/live.m3u8"}`), nil)
	if err != nil || progress.State != "configuration_required" {
		t.Fatalf("begin progress=%#v err=%v", progress, err)
	}
	_, err = host.ContinueResolutionFields(context.Background(), progress.WorkflowID, map[string]json.RawMessage{"required_setting": json.RawMessage(`"provided"`)}, nil, nil)
	if !errors.Is(err, ErrWorkflowFailed) {
		t.Fatalf("continue error=%v, want ErrWorkflowFailed", err)
	}
	if workflows := host.ListWorkflows(); len(workflows) != 0 {
		t.Fatalf("explicit failure remained as an active workflow: %#v", workflows)
	}
	events := host.PendingWorkflowLifecycleEvents()
	if len(events) != 1 || events[0].WorkflowID != progress.WorkflowID || events[0].State != "failed" {
		t.Fatalf("failed terminal events=%#v", events)
	}
}

func TestListWorkflowsHandlesZeroValueHost(t *testing.T) {
	var host Host
	items := host.ListWorkflows()
	if items == nil || len(items) != 0 {
		t.Fatalf("zero-value host list = %#v, want empty non-nil slice", items)
	}
}
