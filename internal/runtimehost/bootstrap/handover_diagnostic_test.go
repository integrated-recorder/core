package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/recorderengine"
)

func TestHandoverDiagnosticUsesStableCodesAndReconcileState(t *testing.T) {
	if got := handoverDiagnosticReason(context.DeadlineExceeded); got != "target_start_timeout" {
		t.Fatalf("deadline reason=%q", got)
	}
	if got := handoverDiagnosticReason(recorderengine.ErrHandoverIdentity); got != "target_not_ready" {
		t.Fatalf("identity reason=%q", got)
	}
	if got := handoverDiagnosticReason(errors.New("password=raw-secret private path /tmp/sentinel")); got != "unknown" {
		t.Fatalf("unknown reason=%q", got)
	}

	controller := &updateController{}
	recordingID := strings.Repeat("b", 32)
	sourceID, targetID := strings.Repeat("a", 32), strings.Repeat("c", 32)
	controller.recordHandoverDiagnostic("prepare_target", context.DeadlineExceeded, sourceID, targetID, recordingID)
	first := controller.handoverDiagnostic
	if first == nil || first.ReasonCode != "target_start_timeout" || first.ReconcileState != "pending" || !first.Retryable || !first.Recoverable || !first.OwnershipRetained || first.ReconcileAttempt != 1 || first.OccurredAt.IsZero() {
		t.Fatalf("first diagnostic=%+v", first)
	}
	controller.recordHandoverDiagnostic("prepare_target", errors.New("credential=raw-secret"), sourceID, targetID, recordingID)
	second := controller.handoverDiagnostic
	if second == nil || second.ReasonCode != "unknown" || second.ReconcileAttempt != 2 {
		t.Fatalf("retry diagnostic=%+v", second)
	}
	encoded, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "raw-secret") || strings.Contains(string(encoded), "credential=") {
		t.Fatalf("raw error escaped sanitized diagnostic: %s", encoded)
	}

	controller.resolveHandoverDiagnostic(recordingID)
	resolved := controller.handoverDiagnostic
	if resolved.ReconcileState != "succeeded" || resolved.Retryable || resolved.ResolvedAt == nil || resolved.ResolvedAt.Before(resolved.OccurredAt) || !resolved.Recoverable || !resolved.OwnershipRetained {
		t.Fatalf("resolved diagnostic=%+v", resolved)
	}
	if time.Since(*resolved.ResolvedAt) > time.Minute {
		t.Fatalf("resolved timestamp is unexpectedly old: %s", resolved.ResolvedAt)
	}
}

func TestHandoverDiagnosticIgnoresUnrelatedRecordingResolution(t *testing.T) {
	controller := &updateController{}
	controller.recordHandoverDiagnostic("prepare_target", context.DeadlineExceeded, strings.Repeat("a", 32), strings.Repeat("c", 32), strings.Repeat("b", 32))
	controller.resolveHandoverDiagnostic(strings.Repeat("d", 32))
	if controller.handoverDiagnostic.ReconcileState != "pending" || controller.handoverDiagnostic.ResolvedAt != nil {
		t.Fatalf("unrelated recording resolution changed diagnostic: %+v", controller.handoverDiagnostic)
	}
}
