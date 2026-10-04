package install

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/integrated-recorder/core/internal/runtimehost/release"
)

var ErrReleaseNotFound = errors.New("installed release was not found")

// RemoveInstalledRelease removes one signed immutable release without
// following links or recursively deleting an untrusted tree. It accepts
// partial cleanup residue only when the signed manifest remains and every
// remaining entry is a regular file named by that manifest. If only one
// metadata file remains, that can only be cleanup residue because artifacts
// and the other metadata file are removed first.
func RemoveInstalledRelease(runtimeRoot, id string, trustedKeys map[string]ed25519.PublicKey) error {
	if runtimeRoot == "" || !filepath.IsAbs(runtimeRoot) || filepath.Clean(runtimeRoot) != runtimeRoot || !installedReleaseIDPattern.MatchString(id) {
		return ErrUnsafeDirectory
	}
	rootInfo, err := os.Lstat(runtimeRoot)
	if errors.Is(err, os.ErrNotExist) {
		return ErrReleaseNotFound
	}
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 || rootInfo.Mode().Perm() != directoryMode {
		return ErrUnsafeDirectory
	}
	releaseRoot := filepath.Join(runtimeRoot, "releases")
	releaseRootInfo, err := os.Lstat(releaseRoot)
	if errors.Is(err, os.ErrNotExist) {
		return ErrReleaseNotFound
	}
	if err != nil || !releaseRootInfo.IsDir() || releaseRootInfo.Mode()&os.ModeSymlink != 0 || releaseRootInfo.Mode().Perm() != directoryMode {
		return ErrUnsafeDirectory
	}
	directory := filepath.Join(releaseRoot, id)
	dirInfo, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return ErrReleaseNotFound
	}
	if err != nil || !dirInfo.IsDir() || dirInfo.Mode()&os.ModeSymlink != 0 || (dirInfo.Mode().Perm() != 0500 && dirInfo.Mode().Perm() != directoryMode) {
		return ErrUnsafeDirectory
	}
	dir, err := os.Open(directory)
	if err != nil {
		return ErrUnsafeDirectory
	}
	openedInfo, statErr := dir.Stat()
	if statErr != nil || !openedInfo.IsDir() || !os.SameFile(dirInfo, openedInfo) {
		_ = dir.Close()
		return ErrUnsafeDirectory
	}
	defer dir.Close()

	entries, err := os.ReadDir(directory)
	if err != nil {
		return ErrUnsafeDirectory
	}
	names := make(map[string]os.FileMode, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		info, statErr := os.Lstat(filepath.Join(directory, name))
		if statErr != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return ErrUnsafeDirectory
		}
		names[name] = info.Mode().Perm()
	}

	allowed := map[string]os.FileMode{"release.json": metadataMode, "release.json.sig": metadataMode}
	var manifest release.Manifest
	manifestBytes, manifestPresent := readRemovalMetadata(directory, names, "release.json", metadataMode)
	signatureBytes, signaturePresent := readRemovalMetadata(directory, names, "release.json.sig", metadataMode)
	if manifestPresent && signaturePresent {
		verified, verifyErr := release.DecodeAndVerifyManifest(manifestBytes, string(signatureBytes), trustedKeys)
		if verifyErr != nil {
			return verifyErr
		}
		if ReleaseDirectoryID(verified.ReleaseVersion, verified.Commit) != id {
			return release.ErrInvalidManifest
		}
		manifest = verified
		for _, artifact := range manifest.Artifacts {
			if !safeFilename(artifact.Filename) {
				return release.ErrInvalidArtifact
			}
			allowed[artifact.Filename] = artifactMode
		}
	} else if len(names) == 0 {
		// Empty directory is the final crash residue after all children were
		// safely removed and immediately before the directory unlink.
	} else if len(names) == 1 && (hasOnlyName(names, "release.json") || hasOnlyName(names, "release.json.sig")) {
		if manifestPresent {
			if err := decodeRemovalManifestIdentity(manifestBytes, id); err != nil {
				return err
			}
		}
	} else {
		return ErrUnsafeDirectory
	}

	for name, mode := range names {
		expectedMode, ok := allowed[name]
		if !ok || mode != expectedMode {
			return ErrUnsafeDirectory
		}
	}

	// The directory is immutable to normal processes. Change permissions via
	// the already-verified descriptor, avoiding a path-following chmod.
	if err := dir.Chmod(directoryMode); err != nil {
		return ErrUnsafeDirectory
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		if name != "release.json" && name != "release.json.sig" {
			ordered = append(ordered, name)
		}
	}
	sort.Strings(ordered)
	if _, ok := names["release.json.sig"]; ok {
		ordered = append(ordered, "release.json.sig")
	}
	if _, ok := names["release.json"]; ok {
		ordered = append(ordered, "release.json")
	}
	for _, name := range ordered {
		// Remove is unlink-like: it never traverses a final symlink. The parent
		// directories are private, and only signed basenames or fixed metadata
		// names can reach this point.
		if err := os.Remove(filepath.Join(directory, name)); err != nil {
			return ErrUnsafeDirectory
		}
	}
	if err := dir.Sync(); err != nil {
		return ErrUnsafeDirectory
	}
	if err := dir.Close(); err != nil {
		return ErrUnsafeDirectory
	}
	if err := os.Remove(directory); err != nil {
		return ErrUnsafeDirectory
	}
	if err := syncDirectory(releaseRoot); err != nil {
		return ErrUnsafeDirectory
	}
	return nil
}

func readRemovalMetadata(directory string, names map[string]os.FileMode, name string, mode os.FileMode) ([]byte, bool) {
	if names[name] != mode {
		return nil, false
	}
	path := filepath.Join(directory, name)
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, false
	}
	file, err := openNoFollow(path)
	if err != nil {
		return nil, false
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) || after.Size() > release.MaxManifestBytes+maxSignatureBytes {
		return nil, false
	}
	limit := int64(release.MaxManifestBytes)
	if name == "release.json.sig" {
		limit = maxSignatureBytes
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, false
	}
	return data, true
}

func hasOnlyName(names map[string]os.FileMode, name string) bool {
	_, ok := names[name]
	return ok && len(names) == 1
}

func decodeRemovalManifestIdentity(data []byte, id string) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest release.Manifest
	if err := decoder.Decode(&manifest); err != nil || ReleaseDirectoryID(manifest.ReleaseVersion, manifest.Commit) != id {
		return ErrUnsafeDirectory
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ErrUnsafeDirectory
	}
	return nil
}
