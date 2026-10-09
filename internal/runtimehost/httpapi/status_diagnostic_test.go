package httpapi

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestStatusValidatesSanitizedHandoverDiagnostic(t *testing.T) {
	status := validStatus()
	status.HandoverDiagnostic = &HandoverDiagnostic{
		RecordingID: strings.Repeat("b", 32), Phase: "prepare_target", ReasonCode: "target_start_timeout",
		SourceGenerationID: strings.Repeat("a", 32), TargetGenerationID: strings.Repeat("c", 32), TargetVersion: "1.2.3",
		OccurredAt: time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC), ReconcileState: "pending",
		Retryable: true, Recoverable: true, OwnershipRetained: true, ReconcileAttempt: 1,
	}
	if err := status.Validate(); err != nil {
		t.Fatalf("valid handover diagnostic rejected: %v", err)
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"handover_diagnostic", "reason_code", "target_start_timeout", "reconcile_attempt", "ownership_retained"} {
		if !strings.Contains(string(encoded), field) {
			t.Fatalf("serialized status omitted %q: %s", field, encoded)
		}
	}
	for _, secret := range []string{"password=sentinel", "token-sentinel", "/private/runtime/path", "stack trace"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("serialized status leaked %q: %s", secret, encoded)
		}
	}

	status.HandoverDiagnostic.ReasonCode = "target failed: password=sentinel"
	if err := status.Validate(); err == nil {
		t.Fatal("untrusted diagnostic text passed reason-code allowlist")
	}
}
