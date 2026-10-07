//go:build darwin || linux

package main

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestConformanceRunnerTimeoutTerminatesProcessGroupDescendants(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "descendant-pid")
	start := time.Now()
	report := runTestAdapterAtMarker(t, "hang-with-descendant", marker, time.Second)
	if report.Passed {
		t.Fatal("hanging adapter unexpectedly passed")
	}
	pid := readDescendantPID(t, marker)
	t.Cleanup(func() { killTestProcess(pid) })
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("timeout cleanup took too long: %s", elapsed)
	}
	time.Sleep(1200 * time.Millisecond)
	if _, err := os.Stat(marker + "-survived"); err == nil {
		t.Fatalf("adapter descendant %d continued running after process-group termination", pid)
	}
}

func TestConformanceRunnerClosesOutputWhenEscapedDescendantHoldsStdout(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "descendant-pid")
	start := time.Now()
	// Allow the helper process to start and publish its descendant PID before
	// the blocked describe request reaches its timeout.
	report := runTestAdapterAtMarker(t, "hang-with-escaped-descendant", marker, 2*time.Second)
	if report.Passed {
		t.Fatal("hanging adapter unexpectedly passed")
	}
	pid := readDescendantPID(t, marker)
	t.Cleanup(func() { killTestProcess(pid) })
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("output-pipe cleanup took too long: %s", elapsed)
	}
	if !processExists(pid) {
		t.Fatal("escaped descendant did not remain alive to exercise inherited stdout cleanup")
	}
}

func TestConformanceAbortClosesReaderWhenEscapedDescendantHoldsStdout(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "descendant-pid")
	binary := writeTestAdapterAtMarker(t, marker)
	setConformanceHelperEnvironment(t, "hang-with-escaped-descendant", marker)
	child, err := startAdapter(binary)
	if err != nil {
		t.Fatalf("start adapter: %v", err)
	}
	t.Cleanup(child.abort)
	if _, err := child.exchange(250*time.Millisecond, "describe", map[string]any{}); err == nil {
		t.Fatal("hanging adapter unexpectedly responded")
	}
	pid := readDescendantPID(t, marker)
	t.Cleanup(func() { killTestProcess(pid) })
	for name, done := range map[string]<-chan struct{}{"process": child.done, "reader": child.readerDone} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("abort did not complete adapter %s promptly", name)
		}
	}
	if !processExists(pid) {
		t.Fatal("escaped descendant did not remain alive while testing output-reader cancellation")
	}
}

func TestConformanceShutdownEOFWaitIsBoundedForEscapedDescendant(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "descendant-pid")
	start := time.Now()
	report := runTestAdapterAtMarker(t, "shutdown-with-escaped-descendant", marker, 2*time.Second)
	if report.Passed {
		t.Fatal("adapter with inherited stdout after shutdown unexpectedly passed")
	}
	pid := readDescendantPID(t, marker)
	t.Cleanup(func() { killTestProcess(pid) })
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("shutdown stdout EOF wait was not bounded: %s", elapsed)
	}
	found := false
	for _, check := range report.Checks {
		if check.Name == "stdout_cleanliness" && !check.Passed {
			found = true
		}
	}
	if !found {
		t.Fatalf("report lacks bounded stdout-close failure: %#v", report)
	}
}

func runTestAdapterAtMarker(t *testing.T, mode, marker string, timeout time.Duration) Report {
	t.Helper()
	binary := writeTestAdapterAtMarker(t, marker)
	return runConformanceWithEnvironment(binary, timeout, mode, marker)
}

func readDescendantPID(t *testing.T, marker string) int {
	t.Helper()
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("read descendant PID: %v", err)
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil || pid <= 0 {
		t.Fatalf("invalid descendant PID: %q", data)
	}
	return pid
}
