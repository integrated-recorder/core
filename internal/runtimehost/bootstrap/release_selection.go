package bootstrap

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"path/filepath"
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

type runtimeRelease struct {
	generationID string
	needsStage   bool
	directory    string
	controlPath  string
	enginePath   string
	appBuild     buildinfo.Info
	manifest     *release.Manifest
}

// selectRuntimeRelease chooses the durable active application generation on
// Host restart. The image-bundled release is trusted by the image boundary;
// any installed release is re-verified from its signed manifest and artifacts.
func selectRuntimeRelease(snapshot generation.Snapshot, bundleBuild buildinfo.Info, bundleDir, runtimeRoot string, trustedKeys map[string]ed25519.PublicKey) (runtimeRelease, error) {
	if activeID := snapshot.ActiveGenerationID; activeID != "" {
		active, ok := snapshot.Generations[activeID]
		if !ok || active.State != generation.StateActive {
			return runtimeRelease{}, errors.New("active runtime generation is inconsistent")
		}
		if !active.SupportsCurrentArchiveFormat() {
			return runtimeRelease{}, generation.ErrArchiveFormatUnsupported
		}
		if active.Version == bundleBuild.Version && strings.EqualFold(active.Commit, bundleBuild.Commit) {
			return runtimeRelease{
				generationID: activeID, directory: bundleDir,
				controlPath: filepath.Join(bundleDir, "control-plane"), enginePath: filepath.Join(bundleDir, "recorder-engine"), appBuild: bundleBuild,
			}, nil
		}
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
			generationID: activeID, directory: installed.Directory,
			controlPath: controlPath, enginePath: enginePath,
			appBuild: buildinfo.Info{Version: manifest.ReleaseVersion, Commit: strings.ToLower(manifest.Commit), BuildTime: manifest.BuildTime, ReleaseChannel: manifest.Channel, RuntimeProtocolVersion: buildinfo.RuntimeProtocolVersion},
			manifest: &manifest,
		}, nil
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
