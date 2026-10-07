package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type ingestCompletion struct {
	err error
}

func readIngestTestPayload(t *testing.T, service *IngestService, recordingID string) *IngestPayload {
	t.Helper()
	payload, err := service.ReadPayload(context.Background(), recordingID, bytes.NewReader([]byte("x")), 8, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func awaitIngestCompletion(t *testing.T, completed <-chan ingestCompletion) error {
	t.Helper()
	select {
	case result := <-completed:
		return result.err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ingest completion")
		return nil
	}
}

func requireIngestFailureDetails(t *testing.T, err error, current, first IngestJobKind, attempts, firstAttempts int) {
	t.Helper()
	var details IngestFailureDetails
	if !errors.As(err, &details) {
		t.Fatalf("failure details missing from %T: %v", err, err)
	}
	if got := details.CurrentJobKind(); got != current {
		t.Errorf("current job kind = %q, want %q", got, current)
	}
	if got := details.FirstFailureJobKind(); got != first {
		t.Errorf("first failure job kind = %q, want %q", got, first)
	}
	if got := details.CurrentAttempts(); got != attempts {
		t.Errorf("current attempts = %d, want %d", got, attempts)
	}
	if got := details.FirstFailureAttempts(); got != firstAttempts {
		t.Errorf("first failure attempts = %d, want %d", got, firstAttempts)
	}
}

func TestIngestFailurePreservesFirstCauseAndJobDetails(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultIngestOptions()
	options.QueueObjects = 2
	options.GlobalBytes = 48
	options.PerRecordingBytes = 24
	options.MaxPayloadBytes = 8
	options.PersistAttempts = 1
	options.RetryBase = 10 * time.Millisecond
	options.RetryMaxBackoff = 10 * time.Millisecond
	service, err := NewIngestService(store, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Errorf("close ingest service: %v", err)
		}
	})
	const recordingID = "first-failure"
	firstCause := errors.New("synthetic first failure secret=redact-me")
	followupBCause := errors.New("synthetic follow-up B failure")
	followupCCause := errors.New("synthetic follow-up C failure")

	entered := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	firstDone := make(chan ingestCompletion, 1)
	followupBDone := make(chan ingestCompletion, 1)
	followupCDone := make(chan ingestCompletion, 1)
	var firstPersistCalls, followupPersistCalls atomic.Int32

	firstPayload := readIngestTestPayload(t, service, recordingID)
	if err := service.SubmitWithKind(context.Background(), firstPayload, IngestJobKindMediaPayload,
		func([]byte) (PayloadResult, error) {
			firstPersistCalls.Add(1)
			close(entered)
			<-release
			return PayloadResult{}, firstCause
		}, func(_ PayloadResult, err error) { firstDone <- ingestCompletion{err: err} }); err != nil {
		firstPayload.Release()
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first persistence callback did not start")
	}

	for _, followup := range []struct {
		kind      IngestJobKind
		cause     error
		completed chan ingestCompletion
	}{
		{IngestJobKindHistoricalMedia, followupBCause, followupBDone},
		{IngestJobKindHistoricalInit, followupCCause, followupCDone},
	} {
		payload := readIngestTestPayload(t, service, recordingID)
		kind, cause, completed := followup.kind, followup.cause, followup.completed
		err := service.SubmitWithKind(context.Background(), payload, kind,
			func([]byte) (PayloadResult, error) {
				followupPersistCalls.Add(1)
				return PayloadResult{}, cause
			}, func(_ PayloadResult, err error) { completed <- ingestCompletion{err: err} })
		if err != nil {
			payload.Release()
			t.Fatal(err)
		}
	}

	close(release)
	firstErr := awaitIngestCompletion(t, firstDone)
	if firstErr == nil || !errors.Is(firstErr, firstCause) || !errors.Is(firstErr, ErrCanonicalCommitFailed) {
		t.Fatalf("first job error = %v, want canonical failure wrapping first cause", firstErr)
	}
	var firstAttemptError attemptCountedError
	if !errors.As(firstErr, &firstAttemptError) || firstAttemptError.Attempts() != 1 {
		t.Fatalf("first job attempts = %#v, want 1", firstAttemptError)
	}
	requireIngestFailureDetails(t, firstErr, IngestJobKindMediaPayload, IngestJobKindMediaPayload, 1, 1)

	for _, followup := range []struct {
		kind      IngestJobKind
		cause     error
		completed <-chan ingestCompletion
	}{
		{IngestJobKindHistoricalMedia, followupBCause, followupBDone},
		{IngestJobKindHistoricalInit, followupCCause, followupCDone},
	} {
		followupErr := awaitIngestCompletion(t, followup.completed)
		if followupErr == nil || !errors.Is(followupErr, firstCause) || !errors.Is(followupErr, ErrCanonicalCommitFailed) {
			t.Fatalf("poisoned %s job error = %v, want first canonical cause", followup.kind, followupErr)
		}
		if errors.Is(followupErr, followup.cause) {
			t.Fatalf("poisoned %s job unexpectedly contains its skipped callback cause", followup.kind)
		}
		if strings.Contains(followupErr.Error(), "redact-me") || strings.Contains(followupErr.Error(), followupBCause.Error()) || strings.Contains(followupErr.Error(), followupCCause.Error()) {
			t.Fatalf("poisoned error exposed a cause: %v", followupErr)
		}
		var attempts attemptCountedError
		if !errors.As(followupErr, &attempts) || attempts.Attempts() != 0 {
			t.Fatalf("poisoned %s attempts = %#v, want 0", followup.kind, attempts)
		}
		requireIngestFailureDetails(t, followupErr, followup.kind, IngestJobKindMediaPayload, 0, 1)
	}
	if got := firstPersistCalls.Load(); got != 1 {
		t.Fatalf("first persistence calls = %d, want 1", got)
	}
	if got := followupPersistCalls.Load(); got != 0 {
		t.Fatalf("poisoned persistence callbacks = %d, want 0", got)
	}
}
