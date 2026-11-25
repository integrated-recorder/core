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
	permissionBinary := filepath.Join(dir, "permission-exit")
	permissionMarker := filepath.Join(dir, "permission-exit.started")
	permissionScript := `: > "` + permissionMarker + `"; echo "$2 DO_NOT_LOG_THIS: operation not permitted" >&2; exit 9`
	if err := os.WriteFile(permissionBinary, []byte("#!/bin/sh\n"+permissionScript+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	permissionResult := make(chan error, 1)
	go func() {
		_, err := Start(context.Background(), StartOptions{
			Binary: permissionBinary, SocketPath: filepath.Join(dir, "permission-exit.sock"), TokenFile: tokenPath,
			StartupTimeout: 5 * time.Second,
		})
		permissionResult <- err
	}()
	startDeadline := time.NewTimer(5 * time.Second)
	tick := time.NewTicker(5 * time.Millisecond)
	started := false
	permissionErr := error(nil)
	resultReceived := false
	for !started {
		select {
		case err := <-permissionResult:
			if _, statErr := os.Stat(permissionMarker); statErr != nil {
				startDeadline.Stop()
				t.Fatalf("permission-exit completed before its script-start marker: %v", err)
			}
			permissionErr = err
			resultReceived = true
			started = true
		case <-tick.C:
			if _, err := os.Stat(permissionMarker); err == nil {
				started = true
			}
		case <-startDeadline.C:
			t.Fatal("permission-exit script was not scheduled before its bounded test deadline")
		}
	}
	tick.Stop()
	startDeadline.Stop()
	if !resultReceived {
		select {
		case permissionErr = <-permissionResult:
		case <-time.After(5 * time.Second):
			t.Fatal("permission-exit did not finish after writing its start marker")
		}
	}
	var permissionDiagnostic *DiagnosticError
	if permissionErr == nil || !errors.As(permissionErr, &permissionDiagnostic) || permissionDiagnostic.Code() != "child_exit_permission_denied" || !errors.Is(permissionErr, ErrProcessExited) {
		t.Fatalf("permission-exit diagnostic = %#v (%v), want child_exit_permission_denied", permissionDiagnostic, permissionErr)
	}
	for _, secret := range []string{dir, tokenPath, "DO_NOT_LOG_THIS"} {
		if strings.Contains(permissionErr.Error(), secret) {
			t.Fatalf("permission-exit diagnostic exposed sensitive value %q: %v", secret, permissionErr)
		}
	}
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
