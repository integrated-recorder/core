// Package runtimehook contains test-only process coordination hooks used by
// production-binary acceptance tests. Normal builds compile a no-op surface.
package runtimehook

import "path/filepath"

// Point identifies one fixed, internal handover/recovery boundary. Keep these
// names private to the runtime implementation; they are not product APIs.
type Point string

// Observation identifies a non-blocking test-only event marker. Unlike a
// Point, an observation never pauses product work.
type Observation string

const (
	BeforeTargetPrepare            Point = "before_target_prepare"
	BeforeHandoverCutoff           Point = "before_handover_cutoff"
	DuringTargetPrepare            Point = "during_target_prepare"
	AfterTargetReady               Point = "after_target_ready"
	AfterSourceAdmissionStop       Point = "after_source_admission_stop"
	DuringSourceDrain              Point = "during_source_drain"
	AfterSourceDrain               Point = "after_source_drain"
	BeforeOwnerCAS                 Point = "before_owner_cas"
	AfterOwnerCAS                  Point = "after_owner_cas"
	BeforeTargetActivation         Point = "before_target_activation"
	BeforeTargetFirstCommit        Point = "before_target_first_commit"
	AfterTargetFirstCommit         Point = "after_target_first_commit"
	BeforeSourceRetirement         Point = "before_source_retirement"
	DuringGenerationLeaseReconcile Point = "during_generation_lease_reconcile"
)

const StaleOwnerCommitRejected Observation = "stale_owner_commit_rejected"

// Arm is a private, test-only selector atomically published after the product
// API has returned the recording ID. The normal product build never reads it.
type Arm struct {
	Point       Point       `json:"point"`
	RecordingID string      `json:"recording_id"`
	Additional  []ArmTarget `json:"additional,omitempty"`
}

// ArmTarget adds another exact boundary/Recording pair to one private test
// arm. This lets process E2E tests coordinate separate cutoff and migration
// boundaries without weakening the exact-ID match.
type ArmTarget struct {
	Point       Point  `json:"point"`
	RecordingID string `json:"recording_id"`
}

func ArmPath(directory string) string { return filepath.Join(directory, "arm.json") }

func ReadyMarkerPath(directory string, point Point, recordingID string) string {
	return filepath.Join(directory, "ready-"+string(point)+"-"+recordingID)
}

func ReleaseMarkerPath(directory string, point Point, recordingID string) string {
	return filepath.Join(directory, "release-"+string(point)+"-"+recordingID)
}

func ObservationMarkerPath(directory string, observation Observation, recordingID string) string {
	return filepath.Join(directory, "observed-"+string(observation)+"-"+recordingID)
}
