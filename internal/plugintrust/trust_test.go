package plugintrust

import "testing"

func TestValidAttestations(t *testing.T) {
	for name, attestation := range map[string]Attestation{
		"bundled":         NewBundled(),
		"official first":  NewOfficialRegistry(FirstParty),
		"official third":  NewOfficialRegistry(ThirdParty),
		"custom registry": NewCustomRegistry(),
		"operator":        NewOperator(),
	} {
		t.Run(name, func(t *testing.T) {
			if err := attestation.Validate(); err != nil {
				t.Fatalf("Validate() = %v", err)
			}
		})
	}
}

func TestInvalidAttestations(t *testing.T) {
	tests := map[string]Attestation{
		"bundled third party": {Provenance: Bundled, Authority: CoreRelease, Publisher: ThirdParty, Reviewed: true},
		"bundled custom":      {Provenance: Bundled, Authority: Custom, Publisher: FirstParty, Reviewed: true},
		"official unknown":    {Provenance: Registry, Authority: Official, Publisher: Unknown, Reviewed: true},
		"official unreviewed": {Provenance: Registry, Authority: Official, Publisher: FirstParty, Reviewed: false},
		"custom first party":  {Provenance: Registry, Authority: Custom, Publisher: FirstParty, Reviewed: false},
		"operator reviewed":   {Provenance: Operator, Authority: Local, Publisher: Unknown, Reviewed: true},
		"legacy admission":    Legacy(),
	}
	for name, attestation := range tests {
		t.Run(name, func(t *testing.T) {
			if err := attestation.Validate(); err == nil {
				t.Fatal("Validate() accepted invalid tuple")
			}
		})
	}
}
