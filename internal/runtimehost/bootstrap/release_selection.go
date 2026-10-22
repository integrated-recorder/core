package bootstrap

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/buildinfo"
	"github.com/integrated-recorder/core/internal/runtimehost/generation"
	"github.com/integrated-recorder/core/internal/runtimehost/install"
	"github.com/integrated-recorder/core/internal/runtimehost/release"
	"github.com/integrated-recorder/core/internal/runtimeipc"
)

const currentManagementSchema = 1

var (
	imageBundleUpgradeIdentityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.+_-]{0,127}$`)
	imageBundleUpgradeCommitPattern   = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

type runtimeRelease struct {
	generationID                         string
	needsStage                           bool
	preserveActivePins                   bool
	imageBundleUpgradeSourceGenerationID string
	directory                            string
	controlPath                          string
	enginePath                           string
	appBuild                             buildinfo.Info
	manifest                             *release.Manifest
}

// selectRuntimeRelease chooses the durable active application generation on
// Host restart. The image-bundled release is trusted by the image boundary;
// any installed release is re-verified from its signed manifest and artifacts.
func selectRuntimeRelease(snapshot generation.Snapshot, bundleBuild buildinfo.Info, bundleDir, runtimeRoot string, trustedKeys map[string]ed25519.PublicKey, bundleUpgradeFrom buildinfo.Info) (runtimeRelease, error) {
	if activeID := snapshot.ActiveGenerationID; activeID != "" {
		active, ok := snapshot.Generations[activeID]
		if !ok || active.State != generation.StateActive {
			return runtimeRelease{}, errors.New("active runtime generation is inconsistent")
		}
		if !active.SupportsCurrentArchiveFormat() {
			return runtimeRelease{}, generation.ErrArchiveFormatUnsupported
		}
		installedPresent, err := installedReleaseDirectoryPresent(runtimeRoot, active.Version, active.Commit)
		if err != nil {
			return runtimeRelease{}, fmt.Errorf("active installed release failed verification: %w", err)
		}
		if installedPresent {
			return selectInstalledRuntimeRelease(active, runtimeRoot, trustedKeys)
		}
		if active.Version == bundleBuild.Version && strings.EqualFold(active.Commit, bundleBuild.Commit) {
			// If the installer is retrying a transition that already persisted the
			// candidate generation, retain the same explicit provenance until the
			// installer commits its metadata. The previous generation remains the
			// exact source of the transition and may still need cold-start cleanup.
			upgradeSourceID := ""
			if bundleUpgradeFrom.Version != "" && snapshot.PreviousGenerationID != "" {
				previous, exists := snapshot.Generations[snapshot.PreviousGenerationID]
				if exists && previous.State == generation.StateDraining && previous.Version == bundleUpgradeFrom.Version && strings.EqualFold(previous.Commit, bundleUpgradeFrom.Commit) {
					upgradeSourceID = previous.ID
				}
			}
			return runtimeRelease{
				generationID: activeID, directory: bundleDir,
				preserveActivePins: upgradeSourceID != "", imageBundleUpgradeSourceGenerationID: upgradeSourceID,
				controlPath: filepath.Join(bundleDir, "control-plane"), enginePath: filepath.Join(bundleDir, "recorder-engine"), appBuild: bundleBuild,
			}, nil
		}
		if bundleUpgradeFrom.Version == active.Version && strings.EqualFold(bundleUpgradeFrom.Commit, active.Commit) {
			// The official installer supplies the prior pinned image identity only
			// during an explicit image upgrade. With no installed release directory,
			// that exact match is provenance for an older image-bundled generation.
			// A new generation is staged from the current image; the old immutable
			// generation and its pins are left intact for lease reconciliation.
			id, err := newGenerationID()
			if err != nil {
				return runtimeRelease{}, err
			}
			return runtimeRelease{
				generationID: id, needsStage: true, preserveActivePins: true,
				imageBundleUpgradeSourceGenerationID: active.ID,
				directory:                            bundleDir, controlPath: filepath.Join(bundleDir, "control-plane"),
				enginePath: filepath.Join(bundleDir, "recorder-engine"), appBuild: bundleBuild,
			}, nil
		}
		return runtimeRelease{}, fmt.Errorf("active installed release failed verification: %w", install.ErrUnsafeDirectory)
	}
	id, err := newGenerationID()
	if err != nil {
		return runtimeRelease{}, err
	}
	return runtimeRelease{
		generationID: id, needsStage: true, directory: bundleDir,
		controlPath: filepath.Join(bundleDir, "control-plane"), enginePath: filepath.Join(bundleDir, "recorder-engine"), appBuild: bundleBuild,
	}, nil
}

func selectInstalledRuntimeRelease(active generation.Generation, runtimeRoot string, trustedKeys map[string]ed25519.PublicKey) (runtimeRelease, error) {
	installed, err := install.InspectInstalledRelease(runtimeRoot, install.ReleaseDirectoryID(active.Version, active.Commit), trustedKeys, currentHostCompatibility())
	if err != nil {
		return runtimeRelease{}, fmt.Errorf("active installed release failed verification: %w", err)
	}
	manifest := installed.Manifest
	if manifest.ReleaseVersion != active.Version || !strings.EqualFold(manifest.Commit, active.Commit) ||
		manifest.ControlProtocolVersion != active.ControlProtocol || manifest.EngineProtocolVersion != active.EngineProtocol ||
		manifest.ArchiveReadMinimum != active.ArchiveReadCompatibility.Minimum || manifest.ArchiveReadMaximum != active.ArchiveReadCompatibility.Maximum ||
		manifest.ArchiveWriteFormat != active.ArchiveWriteFormat {
		return runtimeRelease{}, errors.New("active runtime registry does not match its signed release")
	}
	controlPath, err := installed.ValidateExecutableRole(release.RoleControlPlane)
	if err != nil {
		return runtimeRelease{}, err
	}
	enginePath, err := installed.ValidateExecutableRole(release.RoleRecorderEngine)
	if err != nil {
		return runtimeRelease{}, err
	}
	return runtimeRelease{
		generationID: active.ID, directory: installed.Directory,
		controlPath: controlPath, enginePath: enginePath,
		appBuild: buildinfo.Info{Version: manifest.ReleaseVersion, Commit: strings.ToLower(manifest.Commit), BuildTime: manifest.BuildTime, ReleaseChannel: manifest.Channel, RuntimeProtocolVersion: buildinfo.RuntimeProtocolVersion},
		manifest: &manifest,
	}, nil
}

// installedReleaseDirectoryPresent distinguishes an expected absent
// image-bundled release from an installed-but-invalid signed release. Any
// unsafe or unreadable path is an error, never permission to fall back to the
// image bundle.
func installedReleaseDirectoryPresent(runtimeRoot, version, commit string) (bool, error) {
	if runtimeRoot == "" || !filepath.IsAbs(runtimeRoot) || filepath.Clean(runtimeRoot) != runtimeRoot {
		return false, install.ErrUnsafeDirectory
	}
	rootInfo, err := os.Lstat(runtimeRoot)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return false, install.ErrUnsafeDirectory
	}
	releasesRoot := filepath.Join(runtimeRoot, "releases")
	releasesInfo, err := os.Lstat(releasesRoot)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || !releasesInfo.IsDir() || releasesInfo.Mode()&os.ModeSymlink != 0 {
		return false, install.ErrUnsafeDirectory
	}
	directory := filepath.Join(releasesRoot, install.ReleaseDirectoryID(version, commit))
	if _, err := os.Lstat(directory); errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, install.ErrUnsafeDirectory
	}
	return true, nil
}

func currentHostCompatibility() release.HostCompatibility {
	return release.HostCompatibility{
		Platform: runtime.GOOS, Architecture: runtime.GOARCH,
		RuntimeProtocolVersion: buildinfo.RuntimeProtocolVersion,
		ControlProtocolRange:   release.ProtocolRange{Minimum: runtimeipc.ProtocolVersion, Maximum: runtimeipc.ProtocolVersion},
		EngineProtocolRange:    release.ProtocolRange{Minimum: runtimeipc.ProtocolVersion, Maximum: runtimeipc.ProtocolVersion},
		AdapterProtocolRange:   release.ProtocolRange{Minimum: adapterproto.Version, Maximum: adapterproto.Version},
		ArchiveReadRange:       release.ProtocolRange{Minimum: generation.CurrentArchiveFormatVersion, Maximum: generation.CurrentArchiveFormatVersion},
		ArchiveWriteFormat:     generation.CurrentArchiveFormatVersion, ManagementSchemaVersion: currentManagementSchema,
	}
}
