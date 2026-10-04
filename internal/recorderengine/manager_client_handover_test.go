package recorderengine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/runtimeipc"
)

func TestManagerClientHandoverAcknowledgementChecksRecordingAndEngineIdentity(t *testing.T) {
	owner := recordingowner.Owner{
		RecordingID:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		EngineGeneration: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		WorkerInstance:   "cccccccccccccccccccccccccccccccc",
		Epoch:            5,
	}
	valid := HandoverResult{
		RecordingID: owner.RecordingID, GenerationID: owner.EngineGeneration,
		InstanceID: owner.WorkerInstance, Owner: &owner, Accepted: true,
	}
	if err := validateHandoverAck(valid, owner.RecordingID, owner, true); err != nil {
		t.Fatalf("valid acknowledgement rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*HandoverResult)
	}{
		{name: "recording", mutate: func(result *HandoverResult) { result.RecordingID = "dddddddddddddddddddddddddddddddd" }},
		{name: "generation", mutate: func(result *HandoverResult) { result.GenerationID = "dddddddddddddddddddddddddddddddd" }},
		{name: "instance", mutate: func(result *HandoverResult) { result.InstanceID = "dddddddddddddddddddddddddddddddd" }},
		{name: "owner epoch", mutate: func(result *HandoverResult) { changed := *result.Owner; changed.Epoch++; result.Owner = &changed }},
		{name: "not accepted", mutate: func(result *HandoverResult) { result.Accepted = false }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			result := valid
			test.mutate(&result)
			if err := validateHandoverAck(result, owner.RecordingID, owner, true); !errors.Is(err, ErrHandoverIdentity) {
				t.Fatalf("mismatched acknowledgement error = %v; want ErrHandoverIdentity", err)
			}
		})
	}
	if validHandoverResultIdentity(owner.RecordingID, owner.EngineGeneration, "dddddddddddddddddddddddddddddddd", owner.RecordingID, owner.EngineGeneration, owner.WorkerInstance) {
		t.Fatal("handover response with another Engine instance passed identity validation")
	}
}

func TestManagerClientHandoverErrorsAreTypedAndDoNotEchoRemoteMessage(t *testing.T) {
	stale := normalizeHandoverError(&runtimeipc.RemoteError{Code: "stale_owner", Message: "internal detail"})
	if !errors.Is(stale, ErrHandoverStaleOwner) {
		t.Fatalf("stale error = %v; want ErrHandoverStaleOwner", stale)
	}
	retry := normalizeHandoverError(&runtimeipc.RemoteError{Code: "source_refresh_required", Message: "adapter supplied signed URL that must not escape"})
	if !errors.Is(retry, ErrHandoverSourceRefreshRequired) || strings.Contains(retry.Error(), "signed URL") {
		t.Fatalf("refresh-required code was not safely normalized: %v", retry)
	}
	boundary := normalizeHandoverError(&runtimeipc.RemoteError{Code: "source_boundary_required", Message: "adapter supplied private media URL"})
	if !errors.Is(boundary, ErrHandoverSourceBoundaryRequired) || strings.Contains(boundary.Error(), "private media URL") {
		t.Fatalf("source-boundary code was not safely normalized: %v", boundary)
	}
	secret := "https://media.example/live.m3u8?token=private"
	unsafe := normalizeHandoverError(&runtimeipc.RemoteError{Code: "handover_failed", Message: secret})
	if !errors.Is(unsafe, ErrHandoverOperation) || strings.Contains(unsafe.Error(), secret) {
		t.Fatalf("remote handover error was not safely normalized: %v", unsafe)
	}
	if !errors.Is(normalizeHandoverError(context.DeadlineExceeded), context.DeadlineExceeded) {
		t.Fatal("caller timeout was hidden by handover error normalization")
	}
}
