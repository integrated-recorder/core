//go:build unix

package storageproto

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStartFailureDiagnosticsAreSafeAndDistinguishFailureClasses(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte(strings.Repeat("a", 64)), 0600); err != nil {
		t.Fatal(err)
	}

	assertDiagnostic := func(name, script string, mode os.FileMode, startup time.Duration, wantCode string, wantIs error) {
		t.Helper()
		binary := filepath.Join(dir, name)
		if err := os.WriteFile(binary, []byte("#!/bin/sh\n"+script+"\n"), mode); err != nil {
			t.Fatal(err)
		}
		_, err := Start(context.Background(), StartOptions{
			Binary: binary, SocketPath: filepath.Join(dir, name+".sock"), TokenFile: tokenPath, StartupTimeout: startup,
		})
		if err == nil {
			t.Fatalf("%s unexpectedly started", name)
		}
		var diagnostic *DiagnosticError
		if !errors.As(err, &diagnostic) || diagnostic.Code() != wantCode {
			t.Fatalf("%s diagnostic = %#v (%v), want code %q", name, diagnostic, err, wantCode)
		}
		if wantIs != nil && !errors.Is(err, wantIs) {
			t.Fatalf("%s error %v does not preserve errors.Is(%v)", name, err, wantIs)
		}
		for _, secret := range []string{dir, tokenPath, "DO_NOT_LOG_THIS"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("%s diagnostic exposed sensitive value %q: %v", name, secret, err)
			}
		}
	}

	assertDiagnostic("not-executable", "exit 0", 0600, time.Second, "executable_invalid", nil)
	assertDiagnostic("permission-exit", `echo "$2 DO_NOT_LOG_THIS: operation not permitted" >&2; exit 9`, 0700, time.Second, "child_exit_permission_denied", ErrProcessExited)
	assertDiagnostic("socket-timeout", "exec /bin/sleep 10", 0700, 75*time.Millisecond, "socket_connect_timeout", context.DeadlineExceeded)
	missingInterpreter := filepath.Join(dir, "missing-interpreter")
	if err := os.WriteFile(missingInterpreter, []byte("#!/path/that/does/not/exist\n"), 0700); err != nil {
		t.Fatal(err)
	}
	_, err := Start(context.Background(), StartOptions{
		Binary: missingInterpreter, SocketPath: filepath.Join(dir, "missing-interpreter.sock"), TokenFile: tokenPath,
	})
	var diagnostic *DiagnosticError
	if !errors.As(err, &diagnostic) || diagnostic.Code() != "executable_not_found" || strings.Contains(err.Error(), missingInterpreter) {
		t.Fatalf("exec failure diagnostic = %#v (%v), want safe executable_not_found", diagnostic, err)
	}
}

func TestDiagnosticErrorPreservesSentinelsWithoutExposingCauseText(t *testing.T) {
	private := filepath.Join(t.TempDir(), "private-config-and-token")
	err := NewDiagnosticError("artifact_publication_rename", "permission_denied", os.ErrPermission, errors.New(private))
	if !errors.Is(err, os.ErrPermission) {
		t.Fatal("DiagnosticError lost errors.Is compatibility")
	}
	if strings.Contains(err.Error(), private) || strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("diagnostic exposed wrapped cause text: %v", err)
	}
	stage, code, ok := DiagnosticCode(err)
	if !ok || stage != "artifact_publication_rename" || code != "permission_denied" {
		t.Fatalf("DiagnosticCode() = %q/%q/%t", stage, code, ok)
	}
	unsafe := NewDiagnosticError(private, "permission denied", os.ErrPermission)
	if stage, code, ok := DiagnosticCode(unsafe); !ok || stage != "unknown" || code != "unknown" {
		t.Fatalf("untrusted diagnostic tokens were not rejected: %q/%q/%t", stage, code, ok)
	}
}
