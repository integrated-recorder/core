package discovery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestOwnerAuthorityHistoryMirrorCompactsPrefixesAndRetainsForkCandidates(t *testing.T) {
	first, second := newTestDirectory(t), newTestDirectory(t)
	transport := &directoryTransport{handlers: map[string]http.Handler{
		"one.example": HTTPHandler(first), "two.example": HTTPHandler(second),
	}}
	client, err := NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	base := testAuthorityHistory("1", "", "create", 0x11)
	if _, err := client.PublishOwnerAuthorityHistory(context.Background(), base.Subject, base); err != nil {
		t.Fatalf("publish base: %v", err)
	}
	lookup, err := client.LookupOwnerAuthorityHistories(context.Background(), base.Subject)
	if err != nil || !lookup.Complete || !lookup.Agreed || len(lookup.Histories) != 1 {
		t.Fatalf("base lookup=%+v err=%v", lookup, err)
	}
	extended := testAuthorityHistory("2", "hash-one", "claim", 0x22)
	extended.Events = append(base.Events, extended.Events...)
	if _, err := client.PublishOwnerAuthorityHistory(context.Background(), base.Subject, extended); err != nil {
		t.Fatalf("publish extension: %v", err)
	}
	lookup, err = client.LookupOwnerAuthorityHistories(context.Background(), base.Subject)
	if err != nil || !lookup.Agreed || len(lookup.Histories) != 1 {
		t.Fatalf("prefix was not compacted: histories=%d agreed=%v err=%v", len(lookup.Histories), lookup.Agreed, err)
	}
	var selected OwnerAuthorityHistoryBundle
	if err := json.Unmarshal(lookup.Histories[0], &selected); err != nil || len(selected.Events) != 2 {
		t.Fatalf("lookup did not return the extended head: events=%d err=%v", len(selected.Events), err)
	}
	competing := testAuthorityHistory("2", "hash-one", "competing-claim", 0x33)
	competing.Events = append(base.Events, competing.Events...)
	if _, err := client.PublishOwnerAuthorityHistory(context.Background(), base.Subject, competing); err != nil {
		t.Fatalf("publish competing signed candidate: %v", err)
	}
	lookup, err = client.LookupOwnerAuthorityHistories(context.Background(), base.Subject)
	if err != nil || !lookup.Complete || !lookup.Agreed || len(lookup.Histories) != 2 {
		t.Fatalf("agreed fork candidate mirror lookup=%+v err=%v", lookup, err)
	}
	reopened, err := OpenDirectory(first.root)
	if err != nil {
		t.Fatal(err)
	}
	heads, err := reopened.LookupOwnerAuthorityHistories(base.Subject)
	if err != nil || len(heads) != 2 {
		t.Fatalf("fork candidates did not persist: %d err=%v", len(heads), err)
	}
}

func TestOwnerAuthorityHistoryMirrorDisagreementFailsClosed(t *testing.T) {
	first, second := newTestDirectory(t), newTestDirectory(t)
	base := testAuthorityHistory("1", "", "create", 0x41)
	if err := first.PublishOwnerAuthorityHistory(base.Subject, mustJSON(t, base)); err != nil {
		t.Fatal(err)
	}
	transport := &directoryTransport{handlers: map[string]http.Handler{
		"one.example": HTTPHandler(first), "two.example": HTTPHandler(second),
	}}
	client, err := NewFederationClient([]string{"https://one.example", "https://two.example"}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	lookup, err := client.LookupOwnerAuthorityHistories(context.Background(), base.Subject)
	if !errors.Is(err, ErrNoProvider) || lookup.Complete || lookup.Agreed {
		t.Fatalf("unpublished second mirror accepted: %+v err=%v", lookup, err)
	}
	if _, err := client.PublishOwnerAuthorityHistory(context.Background(), base.Subject, base); err != nil {
		t.Fatal(err)
	}
	competing := testAuthorityHistory("2", "hash-one", "revoke", 0x42)
	competing.Events = append(base.Events, competing.Events...)
	if err := first.PublishOwnerAuthorityHistory(base.Subject, mustJSON(t, competing)); err != nil {
		t.Fatal(err)
	}
	lookup, err = client.LookupOwnerAuthorityHistories(context.Background(), base.Subject)
	if !errors.Is(err, ErrAuthorityDisagrees) || !lookup.Complete || lookup.Agreed || len(lookup.Histories) != 2 {
		t.Fatalf("stale mirror disagreement was accepted: %+v err=%v", lookup, err)
	}
	if _, err := client.PublishOwnerAuthorityHistory(context.Background(), base.Subject, competing); err != nil {
		t.Fatal(err)
	}
	lookup, err = client.LookupOwnerAuthorityHistories(context.Background(), base.Subject)
	if err != nil || !lookup.Complete || !lookup.Agreed {
		t.Fatalf("mirrors did not converge: %+v err=%v", lookup, err)
	}
}

func TestOwnerAuthorityHistoryMirrorRejectsMismatchedSubjectAndSignatureEncoding(t *testing.T) {
	directory := newTestDirectory(t)
	good := testAuthorityHistory("1", "", "create", 0x51)
	if err := directory.PublishOwnerAuthorityHistory(good.Subject, mustJSON(t, good)); err != nil {
		t.Fatal(err)
	}
	wrong := good
	wrong.Subject = "os_CCCCCCCCCCCCCCCCCCCCCC"
	if err := directory.PublishOwnerAuthorityHistory(good.Subject, mustJSON(t, wrong)); !errors.Is(err, ErrAuthorityHistory) {
		t.Fatalf("mismatched subject accepted: %v", err)
	}
	badSig := testAuthorityHistory("1", "", "create", 0x61)
	var signed map[string]any
	if err := json.Unmarshal(badSig.Events[0], &signed); err != nil {
		t.Fatal(err)
	}
	signed["signature"] = "not-base64"
	badSig.Events[0] = mustJSON(t, signed)
	if err := directory.PublishOwnerAuthorityHistory(good.Subject, mustJSON(t, badSig)); !errors.Is(err, ErrAuthorityHistory) {
		t.Fatalf("invalid signature encoding accepted: %v", err)
	}
}

func testAuthorityHistory(sequence, previous, operation string, seed byte) OwnerAuthorityHistoryBundle {
	identityID := "gi_AAAAAAAAAAAAAAAAAAAAAA"
	subjectID := "os_BBBBBBBBBBBBBBBBBBBBBB"
	payload, _ := json.Marshal(map[string]string{"seed": string(rune(seed))})
	event := map[string]any{
		"event_version": "ir.owner-authority.event/1", "identity_id": identityID,
		"identity_head_hash": "sha256:" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"subject_id":         subjectID, "sequence": sequence, "operation": operation,
		"issued_at":     time.Now().UTC().Truncate(time.Second).Format(time.RFC3339),
		"signer":        map[string]string{"kind": "identity", "id": identityID},
		"signer_key_id": "ik_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		"payload":       json.RawMessage(payload),
	}
	if previous != "" {
		event["previous_event_hash"] = previous
	}
	encoded, _ := json.Marshal(map[string]any{"event": event, "signature": base64.StdEncoding.EncodeToString(make([]byte, 64))})
	return OwnerAuthorityHistoryBundle{Version: 1, Identity: identityID, Subject: subjectID, Events: []json.RawMessage{encoded}}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return body
}
