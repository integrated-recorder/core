package authority

import "testing"

func TestValidReplicationPolicyUsesCanonicalBoundedValues(t *testing.T) {
	valid := ReplicationPolicy{MinimumReplicas: "16", GracePeriodSeconds: "2592000", CapacityLimitBytes: "1125899906842624", RelocateAfterGrace: true}
	if !ValidReplicationPolicy(valid) {
		t.Fatal("maximum bounded replication policy was rejected")
	}
	if !ValidReplicationPolicy(DefaultReplicationPolicy()) {
		t.Fatal("default disabled policy was rejected")
	}
	cases := map[string]ReplicationPolicy{
		"empty minimum":        {MinimumReplicas: "", GracePeriodSeconds: "0", CapacityLimitBytes: "0"},
		"leading zero":         {MinimumReplicas: "02", GracePeriodSeconds: "0", CapacityLimitBytes: "0"},
		"replica limit":        {MinimumReplicas: "17", GracePeriodSeconds: "0", CapacityLimitBytes: "0"},
		"grace limit":          {MinimumReplicas: "1", GracePeriodSeconds: "2592001", CapacityLimitBytes: "0"},
		"capacity limit":       {MinimumReplicas: "1", GracePeriodSeconds: "0", CapacityLimitBytes: "1125899906842625"},
		"overflow":             {MinimumReplicas: "18446744073709551616", GracePeriodSeconds: "0", CapacityLimitBytes: "0"},
		"missing capacity cap": {MinimumReplicas: "1", GracePeriodSeconds: "0", CapacityLimitBytes: ""},
	}
	for name, policy := range cases {
		t.Run(name, func(t *testing.T) {
			if ValidReplicationPolicy(policy) {
				t.Fatalf("invalid policy accepted: %+v", policy)
			}
		})
	}
}
