package authority

import "strconv"

const (
	MaxOwnerSubjectReplicaCount  uint64 = 16
	MaxOwnerSubjectGraceSeconds  uint64 = 30 * 24 * 60 * 60
	MaxOwnerSubjectCapacityBytes uint64 = 1 << 50
)

// DefaultReplicationPolicy leaves replication disabled until the Owner
// explicitly chooses a target. This avoids silently placing archive data on
// another node before the Owner has configured retention.
func DefaultReplicationPolicy() ReplicationPolicy {
	return ReplicationPolicy{
		MinimumReplicas: "0", GracePeriodSeconds: "0", CapacityLimitBytes: "0",
		RelocateAfterGrace: false,
	}
}

// ValidReplicationPolicy accepts only bounded canonical decimal values.
// CapacityLimitBytes=0 means unlimited; MinimumReplicas=0 disables scheduling.
func ValidReplicationPolicy(policy ReplicationPolicy) bool {
	minimum, ok := canonicalUint(policy.MinimumReplicas)
	if !ok || minimum > MaxOwnerSubjectReplicaCount {
		return false
	}
	grace, ok := canonicalUint(policy.GracePeriodSeconds)
	if !ok || grace > MaxOwnerSubjectGraceSeconds {
		return false
	}
	capacity, ok := canonicalUint(policy.CapacityLimitBytes)
	return ok && capacity <= MaxOwnerSubjectCapacityBytes
}

func canonicalUint(value string) (uint64, bool) {
	if value == "" {
		return 0, false
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return parsed, err == nil && strconv.FormatUint(parsed, 10) == value
}
