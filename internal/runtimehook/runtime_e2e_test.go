//go:build runtime_e2e

package runtimehook

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const testRecordingID = "0123456789abcdef0123456789abcdef"

func privateTestDirectory(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("", "runtimehook-test-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	return directory
}

func writeTestArm(t *testing.T, directory string, arm Arm, mode os.FileMode) {
	t.Helper()
	data, err := json.Marshal(arm)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ArmPath(directory), data, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(ArmPath(directory), mode); err != nil {
		t.Fatal(err)
	}
}

func configureTestHook(t *testing.T, directory string, point Point) {
	t.Helper()
	t.Setenv(pointEnv, string(point))
	t.Setenv(markerDirEnv, directory)
}

func TestPauseRequiresExactPrivateArmAndRelease(t *testing.T) {
	directory := privateTestDirectory(t)
	point := AfterOwnerCAS
	configureTestHook(t, directory, point)
	writeTestArm(t, directory, Arm{Point: point, RecordingID: "ffffffffffffffffffffffffffffffff"}, 0600)
	if err := Pause(point, testRecordingID); err != nil {
		t.Fatalf("nonmatching Recording ID should not block: %v", err)
	}
	if _, err := os.Lstat(ReadyMarkerPath(directory, point, testRecordingID)); !os.IsNotExist(err) {
		t.Fatalf("nonmatching Recording ID created a marker: %v", err)
	}
	writeTestArm(t, directory, Arm{Point: point, RecordingID: testRecordingID}, 0600)
	result := make(chan error, 1)
	go func() { result <- Pause(point, testRecordingID) }()
	ready := ReadyMarkerPath(directory, point, testRecordingID)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Lstat(ready); err == nil {
			if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 {
				t.Fatalf("ready marker is not a private regular file: %v", info.Mode())
			}
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := os.Lstat(ready); err != nil {
		t.Fatalf("matching hook did not publish its ready marker: %v", err)
	}
	select {
	case err := <-result:
		t.Fatalf("Pause returned before release marker: %v", err)
	default:
	}
	release := ReleaseMarkerPath(directory, point, testRecordingID)
	file, err := os.OpenFile(release, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Pause returned error after safe release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("matching release marker did not unblock Pause")
	}
}

func TestPauseRejectsMalformedSymlinkAndPublicArmFiles(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{name: "malformed", setup: func(t *testing.T, directory string) {
			if err := os.WriteFile(ArmPath(directory), []byte(`{"point":`), 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink", setup: func(t *testing.T, directory string) {
			target := filepath.Join(directory, "target")
			writeTestArm(t, directory, Arm{Point: BeforeOwnerCAS, RecordingID: testRecordingID}, 0600)
			if err := os.Rename(ArmPath(directory), target); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, ArmPath(directory)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "group-readable", setup: func(t *testing.T, directory string) {
			writeTestArm(t, directory, Arm{Point: BeforeOwnerCAS, RecordingID: testRecordingID}, 0644)
		}},
		{name: "unknown-field", setup: func(t *testing.T, directory string) {
			if err := os.WriteFile(ArmPath(directory), []byte(`{"point":"before_owner_cas","recording_id":"0123456789abcdef0123456789abcdef","secret":"x"}`), 0600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := privateTestDirectory(t)
			configureTestHook(t, directory, BeforeOwnerCAS)
			test.setup(t, directory)
			if err := Pause(BeforeOwnerCAS, testRecordingID); err == nil {
				t.Fatal("Pause accepted invalid arm")
			}
		})
	}
}

func TestChildEnvironmentIsAnExplicitAllowlist(t *testing.T) {
	directory := privateTestDirectory(t)
	configureTestHook(t, directory, BeforeTargetFirstCommit)
	t.Setenv("RUNTIME_RESOURCE_TOKEN_FILE", "/private/secret")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	want := []string{pointEnv + "=" + string(BeforeTargetFirstCommit), markerDirEnv + "=" + directory}
	if got := ChildEnvironment(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ChildEnvironment=%q, want exact allowlist %q", got, want)
	}
	for _, item := range ChildEnvironment() {
		if strings.Contains(item, "secret") || strings.Contains(item, "TOKEN") || strings.Contains(item, "AWS_") {
			t.Fatalf("sensitive environment variable was forwarded: %q", item)
		}
	}
}

func TestPauseSupportsMultipleAllowlistedPointsAndExactArmTargets(t *testing.T) {
	directory := privateTestDirectory(t)
	secondRecording := "fedcba9876543210fedcba9876543210"
	configureTestHook(t, directory, BeforeTargetPrepare)
	t.Setenv(pointEnv, string(BeforeTargetPrepare)+","+string(BeforeHandoverCutoff))
	writeTestArm(t, directory, Arm{
		Point: BeforeTargetPrepare, RecordingID: testRecordingID,
		Additional: []ArmTarget{{Point: BeforeHandoverCutoff, RecordingID: secondRecording}},
	}, 0600)
	if got := ChildEnvironment(); !reflect.DeepEqual(got, []string{pointEnv + "=" + string(BeforeTargetPrepare) + "," + string(BeforeHandoverCutoff), markerDirEnv + "=" + directory}) {
		t.Fatalf("multiple point allowlist was not forwarded exactly: %q", got)
	}
	if err := Pause(BeforeHandoverCutoff, testRecordingID); err != nil {
		t.Fatalf("nonmatching additional target should be inert: %v", err)
	}
	result := make(chan error, 1)
	go func() { result <- Pause(BeforeHandoverCutoff, secondRecording) }()
	ready := ReadyMarkerPath(directory, BeforeHandoverCutoff, secondRecording)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Lstat(ready); err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := os.Lstat(ready); err != nil {
		t.Fatalf("matching additional arm did not publish ready marker: %v", err)
	}
	release := ReleaseMarkerPath(directory, BeforeHandoverCutoff, secondRecording)
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("matching additional arm target failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("additional exact arm target did not unblock")
	}
}

func TestPauseRejectsInvalidPrivateDirectory(t *testing.T) {
	root := privateTestDirectory(t)
	directory := filepath.Join(root, "public")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0755); err != nil {
		t.Fatal(err)
	}
	configureTestHook(t, directory, BeforeOwnerCAS)
	writeTestArm(t, directory, Arm{Point: BeforeOwnerCAS, RecordingID: testRecordingID}, 0600)
	if err := Pause(BeforeOwnerCAS, testRecordingID); err == nil {
		t.Fatal("Pause accepted a non-private marker directory")
	}
}

func TestObservePublishesExactRecordingMarkerWithoutMatchingPausePoint(t *testing.T) {
	directory := privateTestDirectory(t)
	configureTestHook(t, directory, BeforeTargetFirstCommit)
	// The event may occur later in the process lifetime than the configured
	// blocking boundary. Only the exact Recording ID is part of the arm match.
	writeTestArm(t, directory, Arm{Point: AfterTargetReady, RecordingID: testRecordingID}, 0600)
	if err := Observe(StaleOwnerCommitRejected, testRecordingID); err != nil {
		t.Fatalf("Observe failed: %v", err)
	}
	path := ObservationMarkerPath(directory, StaleOwnerCommitRejected, testRecordingID)
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("observation marker was not published: %v", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 || info.Size() != 0 {
		t.Fatalf("observation marker has unsafe properties: mode=%v size=%d", info.Mode(), info.Size())
	}
	if err := Observe(StaleOwnerCommitRejected, testRecordingID); err != nil {
		t.Fatalf("repeated observation should be idempotent: %v", err)
	}
}

func TestObserveRequiresMatchingRecordingArmAndAllowlistedObservation(t *testing.T) {
	directory := privateTestDirectory(t)
	configureTestHook(t, directory, BeforeTargetFirstCommit)
	writeTestArm(t, directory, Arm{Point: BeforeTargetFirstCommit, RecordingID: "ffffffffffffffffffffffffffffffff"}, 0600)
	if err := Observe(StaleOwnerCommitRejected, testRecordingID); err != nil {
		t.Fatalf("nonmatching Recording arm should not fail work: %v", err)
	}
	if _, err := os.Lstat(ObservationMarkerPath(directory, StaleOwnerCommitRejected, testRecordingID)); !os.IsNotExist(err) {
		t.Fatalf("nonmatching Recording ID created an observation marker: %v", err)
	}
	if err := Observe(Observation("unlisted_event"), "ffffffffffffffffffffffffffffffff"); err != nil {
		t.Fatalf("unknown observations are inert: %v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(ArmPath(directory)) {
		t.Fatalf("unknown/mismatched observation wrote files: %v", entries)
	}
}
