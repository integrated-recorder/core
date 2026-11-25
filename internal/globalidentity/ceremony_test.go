package globalidentity

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCeremonyTokenIsOriginBoundAndConsumedOnce(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	manager := NewCeremonyManager(nil)
	manager.now = func() time.Time { return now }

	token, err := manager.add(ceremony{
		kind: "registration", origin: "https://recorder.example", identityID: "gi_aaaaaaaaaaaaaaaaaaaaaa",
		expiresAt: now.Add(ceremonyLifetime),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.take(token, "registration", "https://other.example"); !errors.Is(err, ErrCeremonyExpired) {
		t.Fatalf("ceremony accepted a different origin: %v", err)
	}
	if _, err := manager.take(token, "registration", "https://recorder.example"); !errors.Is(err, ErrCeremonyExpired) {
		t.Fatalf("origin-mismatched ceremony token was not consumed: %v", err)
	}

	validToken, err := manager.add(ceremony{
		kind: "login", origin: "https://recorder.example", identityID: "gi_aaaaaaaaaaaaaaaaaaaaaa",
		expiresAt: now.Add(ceremonyLifetime),
	})
	if err != nil {
		t.Fatal(err)
	}
	value, err := manager.take(validToken, "login", "https://recorder.example")
	if err != nil || value.identityID != "gi_aaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("valid ceremony was not returned: value=%+v err=%v", value, err)
	}
	if _, err := manager.take(validToken, "login", "https://recorder.example"); !errors.Is(err, ErrCeremonyExpired) {
		t.Fatalf("ceremony token was reusable: %v", err)
	}
}

func TestCeremonyTokenKindMismatchAndExpiryAreRejected(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	manager := NewCeremonyManager(nil)
	manager.now = func() time.Time { return now }

	wrongKind, err := manager.add(ceremony{kind: "login", origin: "https://recorder.example", expiresAt: now.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.take(wrongKind, "registration", "https://recorder.example"); !errors.Is(err, ErrCeremonyExpired) {
		t.Fatalf("ceremony kind mismatch was accepted: %v", err)
	}
	if _, err := manager.take(wrongKind, "login", "https://recorder.example"); !errors.Is(err, ErrCeremonyExpired) {
		t.Fatalf("kind-mismatched ceremony token was not consumed: %v", err)
	}

	expired, err := manager.add(ceremony{kind: "login", origin: "https://recorder.example", expiresAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.take(expired, "login", "https://recorder.example"); !errors.Is(err, ErrCeremonyExpired) {
		t.Fatalf("expired ceremony was accepted: %v", err)
	}
}

func TestGlobalIdentitySessionCSRFAndLogout(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	genesis, signature, userHandle, credential, challenge := testGenesis(t)
	if err := store.CreateGenesis(genesis, signature, userHandle, credential, challenge); err != nil {
		t.Fatal(err)
	}
	snapshot, exists, err := store.Snapshot()
	if err != nil || !exists {
		t.Fatalf("global identity snapshot: exists=%v err=%v", exists, err)
	}
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	manager := NewCeremonyManager(store)
	manager.now = func() time.Time { return now }
	manager.sessions["session-token"] = identitySession{
		identityID: snapshot.IdentityID, historyHead: snapshot.HeadHash, csrfToken: "csrf-secret",
		expiresAt: now.Add(time.Hour),
	}
	if !manager.ValidCSRF("session-token", "csrf-secret") {
		t.Fatal("valid session CSRF value was rejected")
	}
	if manager.ValidCSRF("session-token", "wrong-csrf") || manager.ValidCSRF("missing-session", "csrf-secret") {
		t.Fatal("invalid session or CSRF value was accepted")
	}
	manager.Logout("session-token")
	if manager.ValidCSRF("session-token", "csrf-secret") {
		t.Fatal("logged-out global identity session remained valid")
	}
}

func TestCeremonyTokenConcurrentConsumptionHasOneWinner(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	manager := NewCeremonyManager(nil)
	manager.now = func() time.Time { return now }
	token, err := manager.add(ceremony{
		kind: "login", origin: "https://recorder.example", expiresAt: now.Add(ceremonyLifetime),
	})
	if err != nil {
		t.Fatal(err)
	}

	const contenders = 24
	var wait sync.WaitGroup
	results := make(chan error, contenders)
	for range contenders {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := manager.take(token, "login", "https://recorder.example")
			results <- err
		}()
	}
	wait.Wait()
	close(results)

	winners, replayRejections := 0, 0
	for err := range results {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, ErrCeremonyExpired):
			replayRejections++
		default:
			t.Fatalf("unexpected concurrent ceremony result: %v", err)
		}
	}
	if winners != 1 || replayRejections != contenders-1 {
		t.Fatalf("ceremony token consumption winners=%d replay_rejections=%d", winners, replayRejections)
	}
}
