// Package plugintrust defines the Host-owned admission provenance attached to
// immutable plugin sets. Plugin descriptors are intentionally not part of
// this model: executable bytes cannot assert how they were admitted.
package plugintrust

import "errors"

type Provenance string

const (
	Bundled            Provenance = "bundled"
	Registry           Provenance = "registry"
	Operator           Provenance = "operator"
	LegacyUnclassified Provenance = "legacy_unclassified"
)

type Authority string

const (
	CoreRelease Authority = "core_release"
	Official    Authority = "official"
	Custom      Authority = "custom"
	Local       Authority = "local"
)

type Publisher string

const (
	FirstParty Publisher = "first_party"
	ThirdParty Publisher = "third_party"
	Unknown    Publisher = "unknown"
)

// Attestation is assigned by Runtime Host from the source used to admit a
// plugin. Reviewed means that the executable was admitted through the named
// distribution process; it is not a claim that native code is sandboxed or
// inherently safe.
type Attestation struct {
	Provenance Provenance `json:"provenance"`
	Authority  Authority  `json:"authority"`
	Publisher  Publisher  `json:"publisher"`
	Reviewed   bool       `json:"reviewed"`
}

var ErrInvalidAttestation = errors.New("plugin trust attestation is invalid")

func (a Attestation) Validate() error {
	switch a.Provenance {
	case Bundled:
		if a.Authority == CoreRelease && a.Publisher == FirstParty && a.Reviewed {
			return nil
		}
	case Registry:
		switch a.Authority {
		case Official:
			if (a.Publisher == FirstParty || a.Publisher == ThirdParty) && a.Reviewed {
				return nil
			}
		case Custom:
			if a.Publisher == Unknown && !a.Reviewed {
				return nil
			}
		}
	case Operator:
		if a.Authority == Local && a.Publisher == Unknown && !a.Reviewed {
			return nil
		}
	}
	return ErrInvalidAttestation
}

// Legacy returns the conservative read-time interpretation for old manifests
// that predate trust attestation. It is never valid for admission or writing.
func Legacy() Attestation {
	return Attestation{Provenance: LegacyUnclassified, Publisher: Unknown}
}

func NewBundled() Attestation {
	return Attestation{Provenance: Bundled, Authority: CoreRelease, Publisher: FirstParty, Reviewed: true}
}

func NewOfficialRegistry(publisher Publisher) Attestation {
	return Attestation{Provenance: Registry, Authority: Official, Publisher: publisher, Reviewed: true}
}

func NewCustomRegistry() Attestation {
	return Attestation{Provenance: Registry, Authority: Custom, Publisher: Unknown, Reviewed: false}
}

func NewOperator() Attestation {
	return Attestation{Provenance: Operator, Authority: Local, Publisher: Unknown, Reviewed: false}
}
