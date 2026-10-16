//go:build runtime_e2e

package runtimehook

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	pointEnv     = "IR_RUNTIME_E2E_FAILPOINT"
	markerDirEnv = "IR_RUNTIME_E2E_MARKER_DIR"
	maxArmBytes  = 512
	waitLimit    = 2 * time.Minute
	pollInterval = 10 * time.Millisecond
)

var (
	validPoint = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	validID    = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

var knownPoints = map[Point]struct{}{
	BeforeTargetPrepare: {}, BeforeHandoverCutoff: {}, DuringTargetPrepare: {}, AfterTargetReady: {},
	AfterSourceAdmissionStop: {}, DuringSourceDrain: {}, AfterSourceDrain: {},
	BeforeOwnerCAS: {}, AfterOwnerCAS: {}, BeforeTargetActivation: {},
	BeforeTargetFirstCommit: {}, AfterTargetFirstCommit: {},
	BeforeSourceRetirement: {}, DuringGenerationLeaseReconcile: {},
}

var knownObservations = map[Observation]struct{}{
	StaleOwnerCommitRejected: {},
}

type config struct {
	points map[Point]struct{}
	dir    string
}

func readConfig() (config, bool) {
	rawPoints, dir := os.Getenv(pointEnv), os.Getenv(markerDirEnv)
	parts := strings.Split(rawPoints, ",")
	if len(parts) == 0 || len(parts) > 16 || dir == "" || !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return config{}, false
	}
	points := make(map[Point]struct{}, len(parts))
	for _, part := range parts {
		point := Point(part)
		if _, ok := knownPoints[point]; !ok || !validPoint.MatchString(part) {
			return config{}, false
		}
		if _, duplicate := points[point]; duplicate {
			return config{}, false
		}
		points[point] = struct{}{}
	}
	return config{points: points, dir: dir}, true
}

// Pause atomically publishes a ready marker and waits for its matching release
// marker. A private arm file supplies the exact generated Recording ID after
// the Control API creates it. This implementation is compiled only with the
// runtime_e2e build tag.
func Pause(point Point, recordingID string) error {
	cfg, enabled := readConfig()
	if _, configured := cfg.points[point]; !enabled || !configured || !validID.MatchString(recordingID) {
		return nil
	}
	if err := validatePrivateDirectory(cfg.dir); err != nil {
		return err
	}
	arm, exists, err := readArm(cfg.dir)
	if err != nil {
		return err
	}
	if !exists || !arm.matches(point, recordingID) {
		return nil
	}
	ready := ReadyMarkerPath(cfg.dir, point, recordingID)
	file, err := os.OpenFile(ready, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("runtime e2e failpoint marker could not be created")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errors.New("runtime e2e failpoint marker could not be synchronized")
	}
	if err := file.Close(); err != nil {
		return errors.New("runtime e2e failpoint marker could not be closed")
	}
	release := ReleaseMarkerPath(cfg.dir, point, recordingID)
	deadline := time.NewTimer(waitLimit)
	defer deadline.Stop()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		if exists, checkErr := safeMarkerExists(release); checkErr != nil {
			return checkErr
		} else if exists {
			return nil
		}
		select {
		case <-deadline.C:
			return fmt.Errorf("runtime e2e failpoint %s timed out", point)
		case <-ticker.C:
		}
	}
}

// Observe atomically publishes an allowlisted empty marker for the exact
// Recording armed by the test harness. It is deliberately independent of the
// selected pause point: a process paused at one boundary can report a later
// event after the Host has restarted. This code is compiled only with the
// runtime_e2e build tag.
func Observe(observation Observation, recordingID string) error {
	cfg, enabled := readConfig()
	if !enabled || !validObservation(observation) || !validID.MatchString(recordingID) {
		return nil
	}
	if err := validatePrivateDirectory(cfg.dir); err != nil {
		return err
	}
	arm, exists, err := readArm(cfg.dir)
	if err != nil {
		return err
	}
	if !exists || arm.RecordingID != recordingID {
		return nil
	}
	path := ObservationMarkerPath(cfg.dir, observation, recordingID)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		markerExists, checkErr := safeObservationMarkerExists(path)
		if checkErr != nil || !markerExists {
			return errors.New("runtime e2e observation marker is invalid")
		}
		return nil
	}
	if err != nil {
		return errors.New("runtime e2e observation marker could not be created")
	}
	if err := file.Chmod(0600); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return errors.New("runtime e2e observation marker permissions could not be set")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return errors.New("runtime e2e observation marker could not be synchronized")
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return errors.New("runtime e2e observation marker could not be closed")
	}
	directory, err := os.Open(cfg.dir)
	if err != nil {
		_ = os.Remove(path)
		return errors.New("runtime e2e observation marker directory is unavailable")
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		_ = os.Remove(path)
		return errors.New("runtime e2e observation marker directory could not be synchronized")
	}
	return nil
}

func validObservation(observation Observation) bool {
	_, ok := knownObservations[observation]
	return ok
}

func (arm Arm) matches(point Point, recordingID string) bool {
	if arm.Point == point && arm.RecordingID == recordingID {
		return true
	}
	for _, target := range arm.Additional {
		if target.Point == point && target.RecordingID == recordingID {
			return true
		}
	}
	return false
}

// ChildEnvironment returns only the fixed point selector and private marker
// directory. No ambient environment or Control/Engine credentials are copied.
func ChildEnvironment() []string {
	cfg, enabled := readConfig()
	if !enabled || validatePrivateDirectory(cfg.dir) != nil {
		return nil
	}
	return []string{pointEnv + "=" + os.Getenv(pointEnv), markerDirEnv + "=" + cfg.dir}
}

func validatePrivateDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 {
		return errors.New("runtime e2e failpoint directory is not private")
	}
	return nil
}

func readArm(directory string) (Arm, bool, error) {
	path := ArmPath(directory)
	linkInfo, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return Arm{}, false, nil
	}
	if err != nil || !linkInfo.Mode().IsRegular() || linkInfo.Mode()&os.ModeSymlink != 0 || linkInfo.Size() < 0 || linkInfo.Size() > maxArmBytes || linkInfo.Mode().Perm() != 0600 {
		return Arm{}, false, errors.New("runtime e2e failpoint arm is invalid")
	}
	file, err := os.Open(path)
	if err != nil {
		return Arm{}, false, errors.New("runtime e2e failpoint arm is unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(linkInfo, info) {
		return Arm{}, false, errors.New("runtime e2e failpoint arm changed during inspection")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxArmBytes+1))
	if err != nil || len(data) > maxArmBytes {
		return Arm{}, false, errors.New("runtime e2e failpoint arm exceeds its limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var arm Arm
	if err := decoder.Decode(&arm); err != nil || !validArm(arm) {
		return Arm{}, false, errors.New("runtime e2e failpoint arm is malformed")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Arm{}, false, errors.New("runtime e2e failpoint arm contains trailing data")
	}
	return arm, true, nil
}

func validArm(arm Arm) bool {
	if !validArmTarget(ArmTarget{Point: arm.Point, RecordingID: arm.RecordingID}) || len(arm.Additional) > 8 {
		return false
	}
	seen := map[ArmTarget]struct{}{{Point: arm.Point, RecordingID: arm.RecordingID}: {}}
	for _, target := range arm.Additional {
		if !validArmTarget(target) {
			return false
		}
		if _, exists := seen[target]; exists {
			return false
		}
		seen[target] = struct{}{}
	}
	return true
}

func validArmTarget(target ArmTarget) bool {
	_, pointOK := knownPoints[target.Point]
	return pointOK && validPoint.MatchString(string(target.Point)) && validID.MatchString(target.RecordingID)
}

func safeMarkerExists(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() != 0 || info.Mode().Perm()&0077 != 0 {
		return false, errors.New("runtime e2e failpoint release marker is invalid")
	}
	return true, nil
}

func safeObservationMarkerExists(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() != 0 || info.Mode().Perm() != 0600 {
		return false, errors.New("runtime e2e observation marker is invalid")
	}
	return true, nil
}
