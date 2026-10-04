package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/authn"
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
	if err := authService.Bootstrap(expected, "runtime-host-cli-test-password"); err != nil {
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
