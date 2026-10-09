package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/applog"
	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/controlplane"
	"github.com/integrated-recorder/core/internal/management"
)

func TestAppendRuntimeAuditPersistsValidatedUserAndSystemActors(t *testing.T) {
	products, err := management.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{products: products}
	ctx := context.Background()
	requests := []controlplane.AuditAppendRequest{
		{Action: controlplane.AuditPluginInstalled, ObjectID: "fixture", ActorType: "user", UserID: "usr-0123456789abcdef0123456789abcdef"},
		{Action: controlplane.AuditRuntimeUpdateStaged, ObjectID: "runtime-update", ActorType: "system"},
	}
	for _, request := range requests {
		if err := s.AppendRuntimeAudit(ctx, request); err != nil {
			t.Fatalf("append actor %s: %v", request.ActorType, err)
		}
	}
	events := products.Audit(10)
	if len(events) != 2 {
		t.Fatalf("stored audit events=%+v", events)
	}
	for _, event := range events {
		if event.Actor == nil {
			t.Fatalf("missing actor on %+v", event)
		}
		if event.Actor.Type == "user" && event.Actor.UserID != requests[0].UserID {
			t.Fatalf("user actor=%+v", event.Actor)
		}
		if event.Actor.Type == "system" && event.Actor.UserID != "" {
			t.Fatalf("system actor=%+v", event.Actor)
		}
	}
	if err := s.AppendRuntimeAudit(ctx, controlplane.AuditAppendRequest{Action: "arbitrary_action", ActorType: "system"}); err == nil {
		t.Fatal("unlisted action was accepted")
	}
	if got := len(products.Audit(10)); got != 2 {
		t.Fatalf("invalid request changed audit store count=%d", got)
	}
}

func TestAuditEndpointCursorPaginationAndIndependentAvailability(t *testing.T) {
	products, err := management.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	handler := NewWithOptions(nil, nil, nil, Options{Management: products})

	// Empty audit remains a successful independent read without recording or
	// storage services configured.
	empty := httptest.NewRecorder()
	handler.ServeHTTP(empty, httptest.NewRequest(http.MethodGet, "/api/audit", nil))
	if empty.Code != http.StatusOK || !strings.Contains(empty.Body.String(), `"items":[]`) {
		t.Fatalf("empty audit response=%d %s", empty.Code, empty.Body.String())
	}

	base := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		actor := &management.AuditActor{Type: management.AuditActorSystem}
		if i%2 == 0 {
			actor = &management.AuditActor{Type: management.AuditActorUser, UserID: "user-stage4-a"}
		}
		if err := products.AppendAudit(management.AuditEvent{
			ID: fmt.Sprintf("audit-%02d", i), Type: "config_changed", At: base.Add(time.Duration(i) * time.Second), ObjectID: "adapter-a", Actor: actor,
		}); err != nil {
			t.Fatal(err)
		}
	}
	type auditResponse struct {
		Items      []management.AuditEvent `json:"items"`
		NextCursor string                  `json:"next_cursor"`
	}
	seen := make(map[string]bool)
	cursor := ""
	for {
		url := "/api/audit?limit=3"
		if cursor != "" {
			url += "&cursor=" + cursor
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, url, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("audit page status=%d body=%s", response.Code, response.Body.String())
		}
		var page auditResponse
		if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		for _, event := range page.Items {
			if seen[event.ID] {
				t.Fatalf("duplicate audit event %s", event.ID)
			}
			want := fmt.Sprintf("audit-%02d", 9-len(seen))
			if event.ID != want {
				t.Fatalf("newest-first order got %s want %s", event.ID, want)
			}
			seen[event.ID] = true
			if event.Actor == nil || (event.Actor.Type != management.AuditActorUser && event.Actor.Type != management.AuditActorSystem) {
				t.Fatalf("audit actor missing: %+v", event)
			}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != 10 {
		t.Fatalf("reached %d of 10 events", len(seen))
	}

	badCursor := httptest.NewRecorder()
	handler.ServeHTTP(badCursor, httptest.NewRequest(http.MethodGet, "/api/audit?cursor=bad", nil))
	if badCursor.Code != http.StatusBadRequest || !strings.Contains(badCursor.Body.String(), `"code":"invalid_audit_cursor"`) {
		t.Fatalf("invalid cursor response=%d %s", badCursor.Code, badCursor.Body.String())
	}
}

func TestAuditMutationUsesAuthenticatedPrincipalAndDegradesOnAppendFailure(t *testing.T) {
	root := t.TempDir()
	products, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	logs := applog.NewStore()
	s := &Server{products: products, logs: logs}
	principal := authn.Principal{UserID: "usr-00000000000000000000000000000001", Login: "owner", Role: authn.RoleOwner}
	r := httptest.NewRequest(http.MethodPost, "/api/watches", nil).WithContext(authn.WithPrincipal(context.Background(), principal))
	if err := s.appendAuditForRequest(r, "watch_created", "watch-stage4-a"); err != nil {
		t.Fatal(err)
	}
	items := products.Audit(10)
	if len(items) != 1 || items[0].Actor == nil || items[0].Actor.Type != management.AuditActorUser || items[0].Actor.UserID != principal.UserID {
		t.Fatalf("request audit actor=%+v", items)
	}

	auditPath := filepath.Join(root, "management", "audit.json")
	if err := os.Remove(auditPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(auditPath, 0700); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	if s.auditMutation(response, r, "watch_updated", "watch-stage4-a") {
		t.Fatal("audit failure reported as recorded")
	}
	if response.Header().Get("X-Audit-Status") != "failed" {
		t.Fatalf("audit failure status header=%q", response.Header().Get("X-Audit-Status"))
	}
	page, err := logs.Query(applog.Query{Component: "audit", Limit: 10})
	if err != nil || len(page.Items) != 1 || !strings.Contains(page.Items[0].Message, "primary mutation") || strings.Contains(page.Items[0].Message, root) {
		t.Fatalf("audit failure diagnostic=%+v err=%v", page, err)
	}
}
