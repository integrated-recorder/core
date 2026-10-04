package install

import (
	"crypto/ed25519"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/integrated-recorder/core/internal/runtimehost/release"
)

var installedReleaseIDPattern = regexp.MustCompile(`^release-[0-9a-f]{64}$`)

// VerifiedRelease is an internal Host view of a private immutable installation.
// Directory and ArtifactPaths are local filesystem details and must never be
// projected through a public API.
type VerifiedRelease struct {
	ID            string
	Directory     string
	Manifest      release.Manifest
	ArtifactPaths map[string]string
}

// ReleaseDirectoryID returns the deterministic immutable installation ID for
// a signed release identity. Callers still verify the signed manifest before
// trusting the corresponding directory.
func ReleaseDirectoryID(version, commit string) string {
	return releaseID(version, commit)
}

// InspectInstalledRelease revalidates a published installation before the Host
// launches any artifact. It checks the signature, compatibility, directory
// shape, immutable permissions, and every artifact size/hash without following
// symlinks. The release ID is the only caller-controlled path component.
func InspectInstalledRelease(runtimeRoot, id string, trustedKeys map[string]ed25519.PublicKey, compatibility release.HostCompatibility) (VerifiedRelease, error) {
	if strings.TrimSpace(runtimeRoot) == "" || !filepath.IsAbs(runtimeRoot) || filepath.Clean(runtimeRoot) != runtimeRoot || !installedReleaseIDPattern.MatchString(id) {
		return VerifiedRelease{}, ErrUnsafeDirectory
	}
	root := filepath.Join(runtimeRoot, "releases")
	for _, path := range []string{runtimeRoot, root} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != directoryMode {
			return VerifiedRelease{}, ErrUnsafeDirectory
		}
	}
	directory := filepath.Join(root, id)
	dirInfo, err := os.Lstat(directory)
	if err != nil || !dirInfo.IsDir() || dirInfo.Mode()&os.ModeSymlink != 0 || dirInfo.Mode().Perm() != 0500 {
		return VerifiedRelease{}, ErrUnsafeDirectory
	}
	manifestBytes, err := readBoundedRegular(filepath.Join(directory, "release.json"), release.MaxManifestBytes)
	if err != nil {
		return VerifiedRelease{}, release.ErrInvalidManifest
	}
	signatureBytes, err := readBoundedRegular(filepath.Join(directory, "release.json.sig"), maxSignatureBytes)
	if err != nil {
		return VerifiedRelease{}, release.ErrInvalidSignature
	}
	for _, name := range []string{"release.json", "release.json.sig"} {
		info, statErr := os.Lstat(filepath.Join(directory, name))
		if statErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != metadataMode {
			return VerifiedRelease{}, ErrUnsafeDirectory
		}
	}
	manifest, err := release.DecodeAndVerifyManifest(manifestBytes, string(signatureBytes), trustedKeys)
	if err != nil {
		return VerifiedRelease{}, err
	}
	if ReleaseDirectoryID(manifest.ReleaseVersion, manifest.Commit) != id {
		return VerifiedRelease{}, release.ErrInvalidManifest
	}
	if err := release.CheckCompatibility(manifest, compatibility); err != nil {
		return VerifiedRelease{}, err
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return VerifiedRelease{}, ErrUnsafeDirectory
	}
	expected := map[string]struct{}{"release.json": {}, "release.json.sig": {}}
	for _, artifact := range manifest.Artifacts {
		if _, duplicate := expected[artifact.Filename]; duplicate {
			return VerifiedRelease{}, release.ErrInvalidManifest
		}
		expected[artifact.Filename] = struct{}{}
	}
	if len(entries) != len(expected) {
		return VerifiedRelease{}, ErrUnsafeDirectory
	}
	paths := make(map[string]string, len(manifest.Artifacts))
	for _, entry := range entries {
		name := entry.Name()
		if _, ok := expected[name]; !ok || entry.Type()&os.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return VerifiedRelease{}, ErrUnsafeDirectory
		}
	}
	for _, artifact := range manifest.Artifacts {
		path := filepath.Join(directory, artifact.Filename)
		info, statErr := os.Lstat(path)
		if statErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != artifactMode {
			return VerifiedRelease{}, ErrUnsafeDirectory
		}
		if err := release.VerifyArtifactFile(path, artifact); err != nil {
			return VerifiedRelease{}, err
		}
		paths[artifact.Role] = path
	}
	return VerifiedRelease{ID: id, Directory: directory, Manifest: manifest, ArtifactPaths: paths}, nil
}

// ValidateExecutableRole returns the verified local executable for one of the
// fixed application process roles. A manifest controls the safe basename but
// cannot introduce a new executable role or a path.
func (r VerifiedRelease) ValidateExecutableRole(role string) (string, error) {
	if role != release.RoleControlPlane && role != release.RoleRecorderEngine {
		return "", errors.New("unsupported application executable role")
	}
	path := r.ArtifactPaths[role]
	if path == "" || filepath.Dir(path) != r.Directory {
		return "", ErrUnsafeDirectory
	}
	return path, nil
}
