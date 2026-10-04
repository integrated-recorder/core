package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

func TestConformanceHelperProcess(t *testing.T) {
	if os.Getenv("IR_CONFORMANCE_HELPER") != "1" {
		return
	}
	os.Exit(runConformanceHelper(os.Getenv("IR_CONFORMANCE_MODE"), os.Getenv("IR_CONFORMANCE_MARKER")))
}

func runConformanceHelper(mode, marker string) int {
	reader := bufio.NewReader(os.Stdin)
	for {
		request, err := adapterproto.ReadRequest(reader)
		if err != nil {
			return 0
		}
		if mode == "hang" && request.Method == adapterproto.MethodDescribe {
			time.Sleep(time.Hour)
			return 0
		}
		if mode == "hang-with-descendant" && request.Method == adapterproto.MethodDescribe {
			child, childErr := startMarkerDescendant(marker + "-survived")
			if childErr == nil {
				_ = os.WriteFile(marker, []byte(strconv.Itoa(child.Process.Pid)), 0600)
			}
			time.Sleep(time.Hour)
			return 0
		}
		if mode == "hang-with-escaped-descendant" && request.Method == adapterproto.MethodDescribe {
			child, childErr := startSleepDescendant(true)
			if childErr == nil {
				_ = os.WriteFile(marker, []byte(strconv.Itoa(child.Process.Pid)), 0600)
			}
			time.Sleep(time.Hour)
			return 0
		}
		if mode == "stdout-garbage" && request.Method == adapterproto.MethodDescribe {
			_, _ = fmt.Fprintln(os.Stdout, "adapter starting")
		}
		if request.Method == adapterproto.MethodDescribe {
			descriptor := conformanceTestDescriptor()
			switch mode {
			case "bad-descriptor":
				descriptor.MediaTypes = nil
			case "bad-descriptor-version":
				descriptor.ProtocolVersion = 2
			case "changed-descriptor":
				count := 0
				if data, readErr := os.ReadFile(marker); readErr == nil {
					count, _ = strconv.Atoi(strings.TrimSpace(string(data)))
				}
				count++
				_ = os.WriteFile(marker, []byte(strconv.Itoa(count)), 0600)
				descriptor.Version = strconv.Itoa(count)
			}
			response, _ := adapterproto.Success(request.ID, descriptor)
			if mode == "bad-response-version" {
				response.ProtocolVersion = 2
			}
			if mode == "bad-id" {
				response.ID = "wrong-id"
			}
			if adapterproto.WriteResponse(os.Stdout, response) != nil {
				return 2
			}
			if mode == "exit-after-describe" {
				return 0
			}
			continue
		}
		if request.Method == adapterproto.MethodShutdown {
			if mode == "shutdown-with-escaped-descendant" {
				child, childErr := startSleepDescendant(true)
				if childErr == nil {
					_ = os.WriteFile(marker, []byte(strconv.Itoa(child.Process.Pid)), 0600)
				}
			}
			response, _ := adapterproto.Success(request.ID, map[string]bool{"ok": true})
			if mode == "bad-shutdown-error" {
				response = adapterproto.Failure(request.ID, "shutdown_failed", "no", nil)
			}
			if adapterproto.WriteResponse(os.Stdout, response) != nil {
				return 2
			}
			if mode == "bad-shutdown-exit" {
				time.Sleep(time.Hour)
			}
			if mode == "extra-after-shutdown" {
				extra, _ := adapterproto.Success("unsolicited", map[string]bool{"unexpected": true})
				if adapterproto.WriteResponse(os.Stdout, extra) != nil {
					return 2
				}
			}
			return 0
		}
		response := adapterproto.Failure(request.ID, "unknown_method", "not supported", nil)
		if adapterproto.WriteResponse(os.Stdout, response) != nil {
			return 2
		}
	}
}

func conformanceTestDescriptor() adapterproto.Descriptor {
	return adapterproto.Descriptor{
		ID: "conformance-test", Name: "Conformance Test", Version: "1.0.0",
		ProtocolVersion: adapterproto.Version, Capabilities: []string{adapterproto.CapabilityResolve},
		InputSchema:         adapterproto.Schema{Fields: []adapterproto.Field{}},
		ConfigurationSchema: adapterproto.Schema{Fields: []adapterproto.Field{}},
		MediaTypes:          []string{"hls"},
	}
}

func runTestAdapter(t *testing.T, mode string, timeout time.Duration) Report {
	t.Helper()
	adapterPath, marker := writeTestAdapter(t)
	return runConformanceWithEnvironment(adapterPath, timeout, mode, marker)
}

func writeTestAdapter(t *testing.T) (string, string) {
	t.Helper()
	marker := filepath.Join(t.TempDir(), "process-count")
	return writeTestAdapterAtMarker(t, marker), marker
}

func writeTestAdapterAtMarker(t *testing.T, marker string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("black-box helper tests require /bin/sh and Unix executable mode bits; unsupported on Windows")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	adapterPath := filepath.Join(t.TempDir(), "integrated-recorder-adapter-test")
	script := "#!/bin/sh\nexec " + shellQuote(binary) + " -test.run=^TestConformanceHelperProcess$\n"
	if err := os.WriteFile(adapterPath, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return adapterPath
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func runConformanceWithEnvironment(binary string, timeout time.Duration, mode, marker string) Report {
	// The adapter shell shim execs the current test binary with one helper test.
	// Environment is inherited by the black-box child process.
	oldHelper, hasHelper := os.LookupEnv("IR_CONFORMANCE_HELPER")
	oldMode, hasMode := os.LookupEnv("IR_CONFORMANCE_MODE")
	oldMarker, hasMarker := os.LookupEnv("IR_CONFORMANCE_MARKER")
	_ = os.Setenv("IR_CONFORMANCE_HELPER", "1")
	_ = os.Setenv("IR_CONFORMANCE_MODE", mode)
	_ = os.Setenv("IR_CONFORMANCE_MARKER", marker)
	defer func() {
		restoreEnv("IR_CONFORMANCE_HELPER", oldHelper, hasHelper)
		restoreEnv("IR_CONFORMANCE_MODE", oldMode, hasMode)
		restoreEnv("IR_CONFORMANCE_MARKER", oldMarker, hasMarker)
	}()
	return runConformance(binary, timeout)
}

func setConformanceHelperEnvironment(t *testing.T, mode, marker string) {
	t.Helper()
	oldHelper, hasHelper := os.LookupEnv("IR_CONFORMANCE_HELPER")
	oldMode, hasMode := os.LookupEnv("IR_CONFORMANCE_MODE")
	oldMarker, hasMarker := os.LookupEnv("IR_CONFORMANCE_MARKER")
	_ = os.Setenv("IR_CONFORMANCE_HELPER", "1")
	_ = os.Setenv("IR_CONFORMANCE_MODE", mode)
	_ = os.Setenv("IR_CONFORMANCE_MARKER", marker)
	t.Cleanup(func() {
		restoreEnv("IR_CONFORMANCE_HELPER", oldHelper, hasHelper)
		restoreEnv("IR_CONFORMANCE_MODE", oldMode, hasMode)
		restoreEnv("IR_CONFORMANCE_MARKER", oldMarker, hasMarker)
	})
}

func restoreEnv(key, value string, existed bool) {
	if existed {
		_ = os.Setenv(key, value)
	} else {
		_ = os.Unsetenv(key)
	}
}

func TestConformanceRunnerAcceptsValidAdapter(t *testing.T) {
	report := runTestAdapter(t, "valid", 5*time.Second)
	if !report.Passed || report.AdapterID != "conformance-test" {
		t.Fatalf("report = %#v", report)
	}
}

func TestConformanceRunnerReadsFinalResponseBeforeProcessExit(t *testing.T) {
	binary, marker := writeTestAdapter(t)
	oldHelper, hasHelper := os.LookupEnv("IR_CONFORMANCE_HELPER")
	oldMode, hasMode := os.LookupEnv("IR_CONFORMANCE_MODE")
	oldMarker, hasMarker := os.LookupEnv("IR_CONFORMANCE_MARKER")
	_ = os.Setenv("IR_CONFORMANCE_HELPER", "1")
	_ = os.Setenv("IR_CONFORMANCE_MODE", "exit-after-describe")
	_ = os.Setenv("IR_CONFORMANCE_MARKER", marker)
	defer func() {
		restoreEnv("IR_CONFORMANCE_HELPER", oldHelper, hasHelper)
		restoreEnv("IR_CONFORMANCE_MODE", oldMode, hasMode)
		restoreEnv("IR_CONFORMANCE_MARKER", oldMarker, hasMarker)
	}()

	child, err := startAdapter(binary)
	if err != nil {
		t.Fatalf("start adapter: %v", err)
	}
	t.Cleanup(child.abort)
	result, err := child.exchange(5*time.Second, adapterproto.MethodDescribe, map[string]any{})
	if err != nil {
		t.Fatalf("read final response before child exit: %v", err)
	}
	var descriptor adapterproto.Descriptor
	if err := json.Unmarshal(result, &descriptor); err != nil || descriptor.ID != "conformance-test" {
		t.Fatalf("descriptor = %#v, err=%v", descriptor, err)
	}
	select {
	case <-child.done:
	case <-time.After(2 * time.Second):
		t.Fatal("adapter did not exit after final response")
	}
}

func TestConformanceRunnerRequiresRegularExecutable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-executable")
	if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	report := runConformance(path, time.Second)
	if report.Passed || len(report.Checks) != 1 || report.Checks[0].Name != "executable" {
		t.Fatalf("report = %#v", report)
	}
}

func TestConformanceRunnerRejectsBrokenAdapters(t *testing.T) {
	tests := []struct {
		mode string
		name string
	}{
		{"stdout-garbage", "stdout garbage"},
		{"bad-id", "wrong request id"},
		{"bad-response-version", "wrong response version"},
		{"bad-descriptor", "invalid descriptor"},
		{"bad-descriptor-version", "wrong descriptor version"},
		{"hang", "response timeout"},
		{"bad-shutdown-error", "shutdown error"},
		{"bad-shutdown-exit", "shutdown exit timeout"},
		{"extra-after-shutdown", "unsolicited shutdown output"},
		{"changed-descriptor", "restart descriptor changed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report := runTestAdapter(t, test.mode, 100*time.Millisecond)
			if report.Passed {
				t.Fatalf("broken adapter passed: %#v", report)
			}
		})
	}
}

func TestConformanceJSONReportIsBoundedAndDoesNotExposeStderr(t *testing.T) {
	binary, marker := writeTestAdapter(t)
	oldHelper, hasHelper := os.LookupEnv("IR_CONFORMANCE_HELPER")
	oldMode, hasMode := os.LookupEnv("IR_CONFORMANCE_MODE")
	oldMarker, hasMarker := os.LookupEnv("IR_CONFORMANCE_MARKER")
	_ = os.Setenv("IR_CONFORMANCE_HELPER", "1")
	_ = os.Setenv("IR_CONFORMANCE_MODE", "valid")
	_ = os.Setenv("IR_CONFORMANCE_MARKER", marker)
	defer func() {
		restoreEnv("IR_CONFORMANCE_HELPER", oldHelper, hasHelper)
		restoreEnv("IR_CONFORMANCE_MODE", oldMode, hasMode)
		restoreEnv("IR_CONFORMANCE_MARKER", oldMarker, hasMarker)
	}()
	var stdout, stderr bytes.Buffer
	if err := command([]string{"--binary", binary, "--json", "--timeout", "5s"}, &stdout, &stderr); err != nil {
		t.Fatalf("CLI failed: %v; stdout=%s stderr=%s", err, stdout.String(), stderr.String())
	}
	var report Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("CLI report is invalid JSON: %v; stdout=%s", err, stdout.String())
	}
	if !report.Passed || len(stdout.Bytes()) > 4096 || strings.Contains(stdout.String(), "stderr") || stderr.Len() != 0 {
		t.Fatalf("unsafe, oversized, or failed report: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}
