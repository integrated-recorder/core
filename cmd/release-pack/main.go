// Command release-pack creates a signed, immutable application release package.
// The signing key is read only from IR_RELEASE_SIGNING_PRIVATE_KEY_BASE64.
package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/buildinfo"
	"github.com/integrated-recorder/core/internal/runtimehost/release"
	"github.com/integrated-recorder/core/internal/runtimeipc"
)

const signingKeyEnv = "IR_RELEASE_SIGNING_PRIVATE_KEY_BASE64"

type options struct {
	version        string
	commit         string
	buildTime      string
	channel        string
	platform       string
	architecture   string
	keyID          string
	output         string
	runtimeHost    string
	controlPlane   string
	recorderEngine string
	adapterRuntime string
}

type sourceArtifact struct {
	role string
	path string
}

func main() {
	if err := run(os.Args[1:], os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, "release packaging failed:", err)
		os.Exit(1)
	}
}

func run(args []string, getenv func(string) string) error {
	flags := flag.NewFlagSet("release-pack", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var opts options
	flags.StringVar(&opts.version, "version", "", "release version (SemVer)")
	flags.StringVar(&opts.commit, "commit", "", "source commit SHA")
	flags.StringVar(&opts.buildTime, "build-time", "", "RFC3339 build timestamp")
	flags.StringVar(&opts.channel, "channel", "", "stable or prerelease")
	flags.StringVar(&opts.platform, "platform", "", "target operating system")
	flags.StringVar(&opts.architecture, "architecture", "", "target architecture")
	flags.StringVar(&opts.keyID, "key-id", "", "trusted signing key identifier")
	flags.StringVar(&opts.output, "output", "", "new target package directory")
	flags.StringVar(&opts.runtimeHost, "runtime-host", "", "Runtime Host executable path")
	flags.StringVar(&opts.controlPlane, "control-plane", "", "Control Plane executable path")
	flags.StringVar(&opts.recorderEngine, "recorder-engine", "", "Recorder Engine executable path")
	flags.StringVar(&opts.adapterRuntime, "adapter-runtime", "", "first-party adapter runtime executable path")
	if err := flags.Parse(args); err != nil {
		return errors.New("invalid command arguments")
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	key, err := decodePrivateKey(getenv(signingKeyEnv))
	if err != nil {
		return err
	}
	return packageRelease(opts, key)
}

func decodePrivateKey(encoded string) (ed25519.PrivateKey, error) {
	if encoded == "" {
		return nil, errors.New("release signing key is not configured")
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(decoded) != ed25519.PrivateKeySize {
		return nil, errors.New("release signing key is invalid")
	}
	privateKey := ed25519.PrivateKey(decoded)
	derived := ed25519.NewKeyFromSeed(privateKey[:ed25519.SeedSize])
	if !bytes.Equal(derived, privateKey) {
		return nil, errors.New("release signing key is invalid")
	}
	return privateKey, nil
}

func packageRelease(opts options, privateKey ed25519.PrivateKey) error {
	if opts.output == "" {
		return errors.New("output directory is required")
	}
	if _, err := time.Parse(time.RFC3339, opts.buildTime); err != nil {
		return errors.New("build time must be an RFC3339 timestamp")
	}
	if opts.channel != "stable" && opts.channel != "prerelease" {
		return errors.New("channel must be stable or prerelease")
	}
	if len(privateKey) != ed25519.PrivateKeySize {
		return errors.New("release signing key is invalid")
	}

	manifest := release.Manifest{
		ManifestSchemaVersion:   release.ManifestSchemaVersion,
		ReleaseVersion:          opts.version,
		Commit:                  opts.commit,
		BuildTime:               opts.buildTime,
		Channel:                 opts.channel,
		KeyID:                   opts.keyID,
		MinimumHostProtocol:     buildinfo.RuntimeProtocolVersion,
		MaximumHostProtocol:     buildinfo.RuntimeProtocolVersion,
		ControlProtocolVersion:  runtimeipc.ProtocolVersion,
		EngineProtocolVersion:   runtimeipc.ProtocolVersion,
		AdapterProtocolMinimum:  adapterproto.Version,
		AdapterProtocolMaximum:  adapterproto.Version,
		ArchiveReadMinimum:      1,
		ArchiveReadMaximum:      1,
		ArchiveWriteEpoch:       1,
		ManagementSchemaMinimum: 1,
		ManagementSchemaMaximum: 1,
		Platform:                opts.platform,
		Architecture:            opts.architecture,
	}
	manifest.Artifacts = make([]release.Artifact, 0, 4)
	output, err := filepath.Abs(opts.output)
	if err != nil {
		return errors.New("output directory is invalid")
	}
	if _, err := os.Lstat(output); err == nil {
		return errors.New("output directory already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("output directory cannot be inspected")
	}
	parent := filepath.Dir(output)
	parentInfo, err := os.Stat(parent)
	if err != nil || !parentInfo.IsDir() {
		return errors.New("output parent directory is unavailable")
	}
	staging, err := os.MkdirTemp(parent, ".release-package-")
	if err != nil {
		return errors.New("release package staging directory could not be created")
	}
	if err := os.Chmod(staging, 0700); err != nil {
		_ = os.RemoveAll(staging)
		return errors.New("release package staging directory could not be secured")
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()

	target := opts.platform + "-" + opts.architecture
	inputs := []sourceArtifact{
		{role: release.RoleRuntimeHost, path: opts.runtimeHost},
		{role: release.RoleControlPlane, path: opts.controlPlane},
		{role: release.RoleRecorderEngine, path: opts.recorderEngine},
		{role: release.RoleAdapterRuntime, path: opts.adapterRuntime},
	}
	for _, input := range inputs {
		filename := input.role + "-" + target
		artifact, err := copyArtifact(staging, input.role, filename, input.path)
		if err != nil {
			return err
		}
		manifest.Artifacts = append(manifest.Artifacts, artifact)
	}
	if err := manifest.Validate(); err != nil {
		return fmt.Errorf("release artifacts are invalid: %w", err)
	}
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return errors.New("release manifest could not be encoded")
	}
	manifestJSON = append(manifestJSON, '\n')
	signature := base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, manifestJSON)) + "\n"
	if err := writeAtomic(staging, "release.json", manifestJSON, 0600); err != nil {
		return err
	}
	if err := writeAtomic(staging, "release.json.sig", []byte(signature), 0600); err != nil {
		return err
	}
	if err := syncDirectory(staging); err != nil {
		return errors.New("release package could not be synchronized")
	}
	if _, err := os.Lstat(output); err == nil {
		return errors.New("output directory already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("output directory cannot be published")
	}
	if err := os.Rename(staging, output); err != nil {
		return errors.New("release package could not be published")
	}
	if err := syncDirectory(parent); err != nil {
		return errors.New("release package parent could not be synchronized")
	}
	published = true
	return nil
}

func copyArtifact(directory, role, filename, sourcePath string) (release.Artifact, error) {
	if sourcePath == "" {
		return release.Artifact{}, fmt.Errorf("path for %s artifact is required", role)
	}
	if !safePackageFilename(filename) {
		return release.Artifact{}, errors.New("generated artifact filename is unsafe")
	}
	before, err := os.Lstat(sourcePath)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Size() < 1 || before.Size() > release.MaxArtifactBytes {
		return release.Artifact{}, fmt.Errorf("%s artifact must be a regular file within the allowed size limit", role)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return release.Artifact{}, fmt.Errorf("%s artifact could not be opened", role)
	}
	defer source.Close()
	opened, err := source.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) || opened.Size() != before.Size() {
		return release.Artifact{}, fmt.Errorf("%s artifact changed while opening", role)
	}
	temporary, err := os.CreateTemp(directory, ".artifact-*.tmp")
	if err != nil {
		return release.Artifact{}, errors.New("artifact staging file could not be created")
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0700); err != nil {
		return release.Artifact{}, errors.New("artifact staging file could not be secured")
	}
	hasher := sha256.New()
	writer := io.MultiWriter(temporary, hasher)
	count, err := io.Copy(writer, io.LimitReader(source, release.MaxArtifactBytes+1))
	if err != nil || count < 1 || count > release.MaxArtifactBytes || count != before.Size() {
		return release.Artifact{}, fmt.Errorf("%s artifact could not be copied within the allowed size limit", role)
	}
	if err := temporary.Sync(); err != nil {
		return release.Artifact{}, errors.New("artifact staging file could not be synchronized")
	}
	if err := temporary.Close(); err != nil {
		return release.Artifact{}, errors.New("artifact staging file could not be closed")
	}
	destination := filepath.Join(directory, filename)
	if err := os.Rename(temporaryPath, destination); err != nil {
		return release.Artifact{}, errors.New("artifact could not be published")
	}
	artifact := release.Artifact{Role: role, Filename: filename, Size: count, SHA256: hex.EncodeToString(hasher.Sum(nil))}
	if err := release.VerifyArtifactFile(destination, artifact); err != nil {
		return release.Artifact{}, fmt.Errorf("%s artifact verification failed", role)
	}
	return artifact, nil
}

func writeAtomic(directory, filename string, data []byte, mode os.FileMode) error {
	temporary, err := os.CreateTemp(directory, ".manifest-*.tmp")
	if err != nil {
		return errors.New("release metadata staging file could not be created")
	}
	path := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(path)
	}()
	if err := temporary.Chmod(mode); err != nil {
		return errors.New("release metadata staging file could not be secured")
	}
	if _, err := temporary.Write(data); err != nil {
		return errors.New("release metadata could not be written")
	}
	if err := temporary.Sync(); err != nil {
		return errors.New("release metadata could not be synchronized")
	}
	if err := temporary.Close(); err != nil {
		return errors.New("release metadata could not be closed")
	}
	if err := os.Rename(path, filepath.Join(directory, filename)); err != nil {
		return errors.New("release metadata could not be published")
	}
	return nil
}

func syncDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func safePackageFilename(filename string) bool {
	if filename == "" || filename == "." || filename == ".." || strings.Contains(filename, "..") || strings.ContainsAny(filename, `/\\:`) {
		return false
	}
	for _, r := range filename {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}
