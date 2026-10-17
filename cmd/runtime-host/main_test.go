package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/management"
)

func TestRuntimeHostSetupCodeSubprocess(t *testing.T) {
	if os.Getenv("IR_RUNTIME_HOST_SETUP_CODE_HELPER") == "1" {
		os.Args = []string{"runtime-host", "setup-code"}
		main()
		os.Exit(0)
	}
	base := os.TempDir()
	if resolved, resolveErr := filepath.EvalSymlinks(base); resolveErr == nil {
		base = resolved
	}
	root, err := os.MkdirTemp(base, "runtime-host-cli-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	authService, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := authn.ReadSetupCode(root)
	if err != nil {
		t.Fatal(err)
	}
	run := func(dataDir string) ([]byte, []byte, error) {
		command := exec.Command(os.Args[0], "-test.run=^TestRuntimeHostSetupCodeSubprocess$")
		command.Env = append(os.Environ(), "IR_RUNTIME_HOST_SETUP_CODE_HELPER=1", "DATA_DIR="+dataDir)
		var stdout, stderr strings.Builder
		command.Stdout, command.Stderr = &stdout, &stderr
		err := command.Run()
		return []byte(stdout.String()), []byte(stderr.String()), err
	}
	stdout, stderr, err := run(root)
	if err != nil || string(stdout) != expected+"\n" || len(stderr) != 0 {
		t.Fatalf("setup-code subprocess stdout=%q stderr=%q err=%v", stdout, stderr, err)
	}
	if err := authService.Bootstrap("owner", expected, "runtime-host-cli-test-password"); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, err = run(root)
	if err == nil || len(stdout) != 0 || strings.Contains(string(stderr), root) || strings.Contains(string(stderr), expected) {
		t.Fatalf("claimed setup-code subprocess stdout=%q stderr=%q err=%v", stdout, stderr, err)
	}
	missing := filepath.Join(root, "missing")
	stdout, stderr, err = run(missing)
	if err == nil || len(stdout) != 0 || strings.Contains(string(stderr), missing) {
		t.Fatalf("missing setup-code subprocess stdout=%q stderr=%q err=%v", stdout, stderr, err)
	}
}

func TestRuntimeHostPasswordRecoveryUsesStdinInvalidatesSessionsAndAudits(t *testing.T) {
	root := t.TempDir()
	service, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	token, err := authn.ReadSetupCode(root)
	if err != nil {
		t.Fatal(err)
	}
	const oldPassword = "runtime-host-old-password"
	const newPassword = "runtime-host-new-password"
	if err := service.Bootstrap("owner", token, oldPassword); err != nil {
		t.Fatal(err)
	}
	oldSession, err := service.Login(oldPassword)
	if err != nil {
		t.Fatal(err)
	}
	input, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteString(newPassword + "\n" + newPassword + "\n"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := resetPasswordFromLocalConsole(root, "owner", input, &output); err != nil {
		t.Fatalf("local recovery: %v", err)
	}
	_ = input.Close()
	if strings.Contains(output.String(), oldPassword) || strings.Contains(output.String(), newPassword) || !strings.Contains(output.String(), "Password reset completed.") {
		t.Fatalf("CLI output disclosed credentials or missed success: %q", output.String())
	}
	reopened, err := authn.OpenWithoutBootstrap(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Authenticate(oldSession.Token); err == nil {
		t.Fatal("pre-recovery session remained valid")
	}
	if _, err := reopened.Login(oldPassword); err == nil {
		t.Fatal("old password remained valid")
	}
	if _, err := reopened.Login(newPassword); err != nil {
		t.Fatalf("new password login: %v", err)
	}
	store, err := management.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	events, _, err := store.AuditPage(20, "")
	if err != nil || len(events) != 1 || events[0].Type != "password_recovered" || events[0].Actor == nil || events[0].Actor.Type != management.AuditActorLocalAdmin {
		t.Fatalf("local recovery audit=%+v err=%v", events, err)
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{oldPassword, newPassword, oldSession.Token, oldSession.CSRFToken} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("security audit disclosed credential material")
		}
	}
}

func TestRuntimeHostPasswordRecoveryRejectsMismatchedStdin(t *testing.T) {
	root := t.TempDir()
	service, err := authn.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	token, err := authn.ReadSetupCode(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Bootstrap("owner", token, "runtime-host-old-password"); err != nil {
		t.Fatal(err)
	}
	input, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_, _ = writer.WriteString("runtime-host-new-password\nother-new-password\n")
	_ = writer.Close()
	var output bytes.Buffer
	if err := resetPasswordFromLocalConsole(root, "owner", input, &output); err == nil {
		t.Fatal("mismatched password confirmation was accepted")
	}
	_ = input.Close()
	if _, err := service.Login("runtime-host-old-password"); err != nil {
		t.Fatalf("mismatched confirmation changed credentials: %v", err)
	}
}
