package adapterhost

import "github.com/integrated-recorder/core/internal/adapterproto"

// DescriptorFingerprint returns the Core's semantic descriptor fingerprint.
// Presentation-only branding is excluded, matching the restart-stability
// check used by the adapter supervisor.
func DescriptorFingerprint(descriptor adapterproto.Descriptor) string {
	return descriptorFingerprint(descriptor)
}
