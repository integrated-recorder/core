// Package storagecatalog stores immutable, Host-owned storage provider
// artifacts and configuration snapshots.
package storagecatalog

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/plugintrust"
	"github.com/integrated-recorder/core/internal/storageproto"
)

const (
	SchemaVersion           = 1
	MaxArtifactBytes        = 512 << 20
	maxManifestBytes        = 64 << 10
	maxStateBytes           = 1 << 20
	maxCatalogItems         = 4096
	maxArtifactAttestations = 5
	probeTimeout            = 6 * time.Second
	copyBufferSize          = 256 << 10
)

var (
	ErrInvalidConfig      = errors.New("storage catalog configuration is invalid")
	ErrUnsafeStore        = errors.New("storage catalog is unsafe")
	ErrInvalidArtifact    = errors.New("storage provider artifact is invalid")
	ErrArtifactMissing    = errors.New("storage provider artifact is unavailable")
	ErrInvalidSet         = errors.New("storage provider set is invalid")
	ErrSetMissing         = errors.New("storage provider set is unavailable")
	ErrAttestationMissing = errors.New("storage provider admission attestation is unavailable")
)

// Expected is the exact provider identity and byte identity required when
// importing a source executable. Size and SHA256 are mandatory.
type Expected struct {
	ID              string
	Version         string
	ProtocolVersion int
	SHA256          string
	Size            int64
}

// Artifact identifies one immutable provider executable and its validated
// descriptor.
type Artifact struct {
	Digest                string `json:"digest"`
	ID                    string `json:"id"`
	Version               string `json:"version"`
	ProtocolVersion       int    `json:"protocol_version"`
	DescriptorFingerprint string `json:"descriptor_fingerprint"`
	Size                  int64  `json:"size"`
}

// SetConfig contains provider configuration values and secrets. Secret bytes
// are persisted only in the set's private mode-0600 config snapshot.
type SetConfig struct {
	Values  map[string]json.RawMessage `json:"values,omitempty"`
	Secrets map[string]string          `json:"secrets,omitempty"`
}

// Set pins one validated artifact to one private configuration snapshot.
type Set struct {
	ID          string                   `json:"id"`
	Artifact    Artifact                 `json:"artifact"`
	Config      SetConfig                `json:"config"`
	Attestation *plugintrust.Attestation `json:"attestation,omitempty"`
}

// EffectiveAttestation returns the persisted Host attestation, or the
// conservative legacy marker for a pre-provenance provider set.
func (s Set) EffectiveAttestation() plugintrust.Attestation {
	if s.Attestation == nil {
		return plugintrust.Legacy()
	}
	return *s.Attestation
}

type setManifest struct {
	SchemaVersion int                      `json:"schema_version"`
	Artifact      Artifact                 `json:"artifact"`
	ConfigSHA256  string                   `json:"config_sha256"`
	Attestation   *plugintrust.Attestation `json:"attestation,omitempty"`
}

// Catalog serializes mutations to the private provider catalog. Runtime Host
// owns one Catalog instance and supplies generation references to GC.
type Catalog struct {
	root string
	mu   sync.Mutex
}

type catalogState struct {
	SchemaVersion         int                                  `json:"schema_version"`
	Installed             []Artifact                           `json:"installed"`
	DesiredSets           map[string]string                    `json:"desired_sets"`
	ArtifactAttestations  map[string][]plugintrust.Attestation `json:"artifact_attestations,omitempty"`
	InstalledAttestations map[string]plugintrust.Attestation   `json:"installed_attestations,omitempty"`
}

// Open creates or validates the private catalog and removes only owned
// leftovers from its staging directory.
func Open(root string) (*Catalog, error) {
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return nil, ErrInvalidConfig
	}
	c := &Catalog{root: root}
	if err := c.ensureLayout(); err != nil {
		return nil, err
	}
	if err := c.cleanStaging(); err != nil {
		return nil, err
	}
	statePath := filepath.Join(c.root, "state.json")
	if _, err := os.Lstat(statePath); errors.Is(err, os.ErrNotExist) {
		if err := c.writeState(catalogState{SchemaVersion: SchemaVersion, Installed: []Artifact{}, DesiredSets: map[string]string{}}); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, ErrUnsafeStore
	}
	if _, err := c.readState(); err != nil {
		return nil, err
	}
	return c, nil
}

// OpenReadOnly validates an existing catalog without creating files or
// cleaning staging. Application generations use this when opening a pinned
// provider so a Control or Engine startup cannot delete an in-flight Host
// import operation's staging data.
func OpenReadOnly(root string) (*Catalog, error) {
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return nil, ErrInvalidConfig
	}
	c := &Catalog{root: root}
	if err := c.checkLayout(); err != nil {
		return nil, err
	}
	if _, err := c.readState(); err != nil {
		return nil, err
	}
	return c, nil
}

// Import snapshots, hashes, and probes a regular executable before publishing
// its immutable content-addressed artifact.
func (c *Catalog) Import(ctx context.Context, sourcePath string, expected Expected) (Artifact, error) {
	if c == nil || ctx == nil || sourcePath == "" || !filepath.IsAbs(sourcePath) ||
		!validDigest(expected.SHA256) || expected.Size <= 0 || expected.Size > MaxArtifactBytes ||
		expected.ID == "" || expected.Version == "" || expected.ProtocolVersion != storageproto.Version {
		return Artifact{}, ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkLayout(); err != nil {
		return Artifact{}, err
	}
	if err := ctx.Err(); err != nil {
		return Artifact{}, err
	}

	before, err := os.Lstat(sourcePath)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Mode().Perm()&0111 == 0 || before.Size() != expected.Size {
		return Artifact{}, ErrInvalidArtifact
	}
	input, err := openSourceNoFollow(sourcePath)
	if err != nil {
		return Artifact{}, ErrInvalidArtifact
	}
	defer input.Close()
	opened, err := input.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) || opened.Size() != expected.Size || !opened.ModTime().Equal(before.ModTime()) {
		return Artifact{}, ErrInvalidArtifact
	}

	stageDir, err := os.MkdirTemp(filepath.Join(c.root, "staging"), "import-")
	if err != nil {
		return Artifact{}, ErrUnsafeStore
	}
	defer func() { _ = removeTreeOwned(stageDir) }()
	stageBinary := filepath.Join(stageDir, "provider")
	output, err := os.OpenFile(stageBinary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return Artifact{}, ErrUnsafeStore
	}
	h := sha256.New()
	written, copyErr := copyContext(ctx, io.MultiWriter(output, h), input, make([]byte, copyBufferSize), MaxArtifactBytes)
	if copyErr == nil && written != expected.Size {
		copyErr = errors.New("source size changed")
	}
	if copyErr == nil {
		copyErr = output.Sync()
	}
	closeErr := output.Close()
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		if ctx.Err() != nil {
			return Artifact{}, ctx.Err()
		}
		return Artifact{}, ErrInvalidArtifact
	}
	digest := hex.EncodeToString(h.Sum(nil))
	if digest != expected.SHA256 {
		return Artifact{}, ErrInvalidArtifact
	}
	afterPath, pathErr := os.Lstat(sourcePath)
	afterFD, fdErr := input.Stat()
	if pathErr != nil || fdErr != nil || afterPath.Mode()&os.ModeSymlink != 0 || !afterPath.Mode().IsRegular() ||
		!os.SameFile(before, afterPath) || !os.SameFile(opened, afterFD) || afterPath.Size() != expected.Size || afterFD.Size() != expected.Size ||
		!afterPath.ModTime().Equal(before.ModTime()) || !afterFD.ModTime().Equal(opened.ModTime()) || afterPath.Mode().Perm()&0111 == 0 || afterFD.Mode().Perm()&0111 == 0 {
		return Artifact{}, ErrInvalidArtifact
	}
	if err := ctx.Err(); err != nil {
		return Artifact{}, err
	}
	if err := os.Chmod(stageBinary, 0500); err != nil {
		return Artifact{}, ErrUnsafeStore
	}

	descriptor, probeErr := probeExecutable(ctx, stageBinary)
	if probeErr != nil || descriptor.ID != expected.ID || descriptor.Version != expected.Version || descriptor.ProtocolVersion != expected.ProtocolVersion {
		if ctx.Err() != nil {
			return Artifact{}, ctx.Err()
		}
		return Artifact{}, fmt.Errorf("%w: descriptor probe rejected (probe=%v id=%q version=%q protocol=%d)", ErrInvalidArtifact, probeErr, descriptor.ID, descriptor.Version, descriptor.ProtocolVersion)
	}
	fingerprint, err := storageproto.Fingerprint(descriptor)
	if err != nil {
		return Artifact{}, ErrInvalidArtifact
	}
	artifact := Artifact{Digest: digest, ID: descriptor.ID, Version: descriptor.Version, ProtocolVersion: descriptor.ProtocolVersion, DescriptorFingerprint: fingerprint, Size: written}
	if err := c.publishArtifact(stageDir, artifact, descriptor); err != nil {
		return Artifact{}, err
	}
	return artifact, nil
}

// ImportBundled imports a Host-image-bundled provider after deriving its
// declared version from the executable itself. Import still snapshots, hashes,
// and probes the exact staged bytes before publication; the initial probe is
// only used to build the exact Expected identity for that authoritative path.
func (c *Catalog) ImportBundled(ctx context.Context, sourcePath, expectedID string) (Artifact, error) {
	if ctx == nil || expectedID == "" {
		return Artifact{}, ErrInvalidConfig
	}
	descriptor, err := probeExecutable(ctx, sourcePath)
	if err != nil || descriptor.ID != expectedID || descriptor.ProtocolVersion != storageproto.Version {
		return Artifact{}, ErrInvalidArtifact
	}
	size, digest, err := stableExecutableDigest(ctx, sourcePath)
	if err != nil {
		return Artifact{}, ErrInvalidArtifact
	}
	return c.Import(ctx, sourcePath, Expected{
		ID: expectedID, Version: descriptor.Version, ProtocolVersion: storageproto.Version,
		SHA256: digest, Size: size,
	})
}

func probeExecutable(ctx context.Context, binary string) (storageproto.Descriptor, error) {
	if ctx == nil || !filepath.IsAbs(binary) || filepath.Clean(binary) != binary {
		return storageproto.Descriptor{}, ErrInvalidArtifact
	}
	probeDir, err := os.MkdirTemp("", "ir-storage-probe-")
	if err != nil {
		return storageproto.Descriptor{}, ErrUnsafeStore
	}
	defer func() { _ = removeTreeOwned(probeDir) }()
	socketPath, tokenPath, err := createProbeCredentials(probeDir)
	if err != nil {
		return storageproto.Descriptor{}, ErrUnsafeStore
	}
	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	client, err := storageproto.Start(probeCtx, storageproto.StartOptions{
		Binary: binary, SocketPath: socketPath, TokenFile: tokenPath, StartupTimeout: probeTimeout,
	})
	if err != nil {
		if ctx.Err() != nil {
			return storageproto.Descriptor{}, ctx.Err()
		}
		return storageproto.Descriptor{}, ErrInvalidArtifact
	}
	descriptor, describeErr := client.Describe(probeCtx)
	closeErr := client.Close()
	if describeErr != nil || closeErr != nil || storageproto.ValidateDescriptor(descriptor) != nil {
		return storageproto.Descriptor{}, ErrInvalidArtifact
	}
	return descriptor, nil
}

func stableExecutableDigest(ctx context.Context, sourcePath string) (int64, string, error) {
	if ctx == nil || !filepath.IsAbs(sourcePath) || filepath.Clean(sourcePath) != sourcePath {
		return 0, "", ErrInvalidArtifact
	}
	before, err := os.Lstat(sourcePath)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Mode().Perm()&0111 == 0 || before.Size() <= 0 || before.Size() > MaxArtifactBytes {
		return 0, "", ErrInvalidArtifact
	}
	input, err := openSourceNoFollow(sourcePath)
	if err != nil {
		return 0, "", ErrInvalidArtifact
	}
	defer input.Close()
	opened, err := input.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) || opened.Size() != before.Size() {
		return 0, "", ErrInvalidArtifact
	}
	h := sha256.New()
	written, err := copyContext(ctx, h, input, make([]byte, copyBufferSize), MaxArtifactBytes)
	if err != nil || written != before.Size() {
		return 0, "", ErrInvalidArtifact
	}
	afterPath, pathErr := os.Lstat(sourcePath)
	afterFD, fdErr := input.Stat()
	if pathErr != nil || fdErr != nil || !afterPath.Mode().IsRegular() || afterPath.Mode()&os.ModeSymlink != 0 ||
		!os.SameFile(before, afterPath) || !os.SameFile(opened, afterFD) || afterPath.Size() != before.Size() || afterFD.Size() != opened.Size() ||
		!afterPath.ModTime().Equal(before.ModTime()) || !afterFD.ModTime().Equal(opened.ModTime()) {
		return 0, "", ErrInvalidArtifact
	}
	return written, hex.EncodeToString(h.Sum(nil)), nil
}

// CreateSet validates config against the artifact schema, canonicalizes the
// private snapshot, and publishes it under its content identity.
func (c *Catalog) CreateSet(artifactDigest string, config SetConfig) (Set, error) {
	return c.CreateSetWithAttestation(artifactDigest, config, plugintrust.NewOperator())
}

// CreateSetWithAttestation publishes an immutable provider configuration set
// with Host-selected admission provenance. The attestation is kept in the set
// manifest, separate from the content-addressed executable artifact.
func (c *Catalog) CreateSetWithAttestation(artifactDigest string, config SetConfig, attestation plugintrust.Attestation) (Set, error) {
	if c == nil || !validDigest(artifactDigest) {
		return Set{}, ErrInvalidConfig
	}
	if attestation.Validate() != nil {
		return Set{}, ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.createSetLocked(artifactDigest, config, attestation)
}

// CreateInstalledSet atomically snapshots the currently installed artifact
// and its durable selected admission attestation. It is intended for the
// configure/probe path after installation, including when the Registry is
// unavailable.
func (c *Catalog) CreateInstalledSet(providerID string, config SetConfig) (Set, error) {
	if c == nil || !validProviderID(providerID) {
		return Set{}, ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.readState()
	if err != nil {
		return Set{}, err
	}
	artifact, found := installedArtifact(state.Installed, providerID)
	if !found {
		return Set{}, ErrInvalidArtifact
	}
	attestation, found := state.InstalledAttestations[providerID]
	if !found {
		return Set{}, ErrAttestationMissing
	}
	return c.createSetLocked(artifact.Digest, config, attestation)
}

func (c *Catalog) createSetLocked(artifactDigest string, config SetConfig, attestation plugintrust.Attestation) (Set, error) {
	artifact, descriptor, err := c.loadArtifact(artifactDigest)
	if err != nil {
		return Set{}, err
	}
	canonicalConfig, configBytes, err := canonicalConfig(config)
	if err != nil || storageproto.ValidateConfigForSchema(descriptor.ConfigurationSchema, toProtocolConfig(canonicalConfig)) != nil {
		return Set{}, ErrInvalidConfig
	}
	if int64(len(configBytes)) > storageproto.MaxConfigBytes {
		return Set{}, ErrInvalidConfig
	}
	configSum := sha256.Sum256(configBytes)
	attestationCopy := attestation
	manifestBytes, err := encodeSetManifest(artifact, hex.EncodeToString(configSum[:]), &attestationCopy)
	if err != nil || len(manifestBytes) > maxManifestBytes {
		return Set{}, ErrInvalidConfig
	}
	id := digestBytes(manifestBytes)
	set := Set{ID: id, Artifact: artifact, Config: canonicalConfig, Attestation: &attestationCopy}
	setRoot := filepath.Join(c.root, "sets")
	target := filepath.Join(setRoot, id)
	if _, err := os.Lstat(target); err == nil {
		loaded, loadErr := c.loadSet(id)
		if loadErr != nil || !sameSetContent(loaded, set) {
			return Set{}, ErrUnsafeStore
		}
		return loaded, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return Set{}, ErrUnsafeStore
	}
	stageDir, err := os.MkdirTemp(filepath.Join(c.root, "staging"), "set-")
	if err != nil {
		return Set{}, ErrUnsafeStore
	}
	defer func() { _ = removeTreeOwned(stageDir) }()
	if err := writePrivateFile(filepath.Join(stageDir, "manifest.json"), manifestBytes, 0400); err != nil {
		return Set{}, ErrUnsafeStore
	}
	if err := writePrivateFile(filepath.Join(stageDir, "config.json"), configBytes, 0600); err != nil {
		return Set{}, ErrUnsafeStore
	}
	if err := syncDirectory(stageDir); err != nil {
		return Set{}, ErrUnsafeStore
	}
	if err := os.Rename(stageDir, target); err != nil {
		if _, statErr := os.Lstat(target); statErr == nil {
			loaded, loadErr := c.loadSet(id)
			if loadErr == nil && sameSetContent(loaded, set) {
				return loaded, nil
			}
		}
		return Set{}, ErrUnsafeStore
	}
	if err := syncDirectory(setRoot); err != nil {
		return Set{}, ErrUnsafeStore
	}
	return set, nil
}

// LoadSet validates and loads one durable immutable configuration snapshot.
func (c *Catalog) LoadSet(id string) (Set, error) {
	if c == nil {
		return Set{}, ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loadSet(id)
}

// ArtifactPath returns the verified executable path for trusted internal
// Runtime Host use.
func (c *Catalog) ArtifactPath(digest string) (string, error) {
	if c == nil || !validDigest(digest) {
		return "", ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, _, err := c.loadArtifact(digest); err != nil {
		return "", err
	}
	return filepath.Join(c.root, "artifacts", digest, "provider"), nil
}

// ConfigForSet returns a detached copy of the private config snapshot. It is
// intended only for trusted Runtime Host Engine launch code.
func (c *Catalog) ConfigForSet(id string) (SetConfig, error) {
	set, err := c.LoadSet(id)
	if err != nil {
		return SetConfig{}, err
	}
	return cloneConfig(set.Config), nil
}

// DescribeArtifact returns the validated stored descriptor and immutable
// artifact identity for provider configuration UI and Host lifecycle code.
func (c *Catalog) DescribeArtifact(digest string) (Artifact, storageproto.Descriptor, error) {
	if c == nil || !validDigest(digest) {
		return Artifact{}, storageproto.Descriptor{}, ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loadArtifact(digest)
}

// Install adds or replaces the provider's durable desired-artifact marker.
// The artifact must already have been imported into this catalog.
func (c *Catalog) Install(artifact Artifact) error {
	return c.install(artifact, nil)
}

// InstallWithAttestation persists the selected admission evidence separately
// from the content-only Artifact. The evidence is available to a later
// configuration operation even if the Registry is no longer reachable.
func (c *Catalog) InstallWithAttestation(artifact Artifact, attestation plugintrust.Attestation) error {
	if attestation.Validate() != nil {
		return ErrInvalidConfig
	}
	return c.install(artifact, &attestation)
}

func (c *Catalog) install(artifact Artifact, attestation *plugintrust.Attestation) error {
	if c == nil {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stored, _, err := c.loadArtifact(artifact.Digest)
	if err != nil || stored != artifact {
		return ErrInvalidArtifact
	}
	state, err := c.readState()
	if err != nil {
		return err
	}
	previous, previouslyInstalled := installedArtifact(state.Installed, artifact.ID)
	state.Installed = replaceArtifact(state.Installed, artifact)
	if attestation != nil {
		if err := rememberArtifactAttestation(&state, artifact.Digest, *attestation); err != nil {
			return err
		}
		if state.InstalledAttestations == nil {
			state.InstalledAttestations = map[string]plugintrust.Attestation{}
		}
		state.InstalledAttestations[artifact.ID] = *attestation
	} else if !previouslyInstalled || previous.Digest != artifact.Digest {
		// An unclassified replacement must not inherit the old artifact's trust.
		delete(state.InstalledAttestations, artifact.ID)
	}
	return c.writeState(state)
}

// InstalledAttestation returns the source evidence selected for the currently
// installed artifact identity. found is false for pre-provenance or otherwise
// unclassified installed records; callers must not infer a stronger class.
func (c *Catalog) InstalledAttestation(providerID string) (attestation plugintrust.Attestation, found bool, err error) {
	if c == nil || !validProviderID(providerID) {
		return plugintrust.Attestation{}, false, ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.readState()
	if err != nil {
		return plugintrust.Attestation{}, false, err
	}
	if _, ok := installedArtifact(state.Installed, providerID); !ok {
		return plugintrust.Attestation{}, false, ErrInvalidConfig
	}
	attestation, found = state.InstalledAttestations[providerID]
	return attestation, found, nil
}

// AttestationsForArtifact returns the distinct Host admission evidence
// recorded for exact immutable bytes. The digest identifies bytes only and
// never selects or promotes one attestation over another.
func (c *Catalog) AttestationsForArtifact(digest string) ([]plugintrust.Attestation, error) {
	if c == nil || !validDigest(digest) {
		return nil, ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, _, err := c.loadArtifact(digest); err != nil {
		return nil, err
	}
	state, err := c.readState()
	if err != nil {
		return nil, err
	}
	return append([]plugintrust.Attestation(nil), state.ArtifactAttestations[digest]...), nil
}

// Installed returns the durable desired provider inventory sorted by ID.
func (c *Catalog) Installed() ([]Artifact, error) {
	if c == nil {
		return nil, ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.readState()
	if err != nil {
		return nil, err
	}
	return append([]Artifact(nil), state.Installed...), nil
}

// Uninstall removes desired installation and configuration selection markers.
// It leaves immutable set and artifact bytes for reference-aware GC.
func (c *Catalog) Uninstall(id string) error {
	if c == nil || !validProviderID(id) {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.readState()
	if err != nil {
		return err
	}
	installed := state.Installed[:0]
	for _, artifact := range state.Installed {
		if artifact.ID != id {
			installed = append(installed, artifact)
		}
	}
	state.Installed = installed
	delete(state.DesiredSets, id)
	delete(state.InstalledAttestations, id)
	return c.writeState(state)
}

// SelectDesiredSet durably selects an immutable configured set for a provider.
// It does not change the active generation; Generation remains authoritative.
func (c *Catalog) SelectDesiredSet(providerID, setID string) error {
	if c == nil || !validProviderID(providerID) || !validDigest(setID) {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	set, err := c.loadSet(setID)
	if err != nil {
		return err
	}
	if set.Artifact.ID != providerID {
		return ErrInvalidSet
	}
	state, err := c.readState()
	if err != nil {
		return err
	}
	if !hasInstalledID(state.Installed, providerID) {
		return ErrInvalidConfig
	}
	state.DesiredSets[providerID] = setID
	return c.writeState(state)
}

// DesiredSet returns the selected immutable configured set for a provider.
func (c *Catalog) DesiredSet(providerID string) (Set, error) {
	if c == nil || !validProviderID(providerID) {
		return Set{}, ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state, err := c.readState()
	if err != nil {
		return Set{}, err
	}
	setID, ok := state.DesiredSets[providerID]
	if !ok {
		return Set{}, ErrSetMissing
	}
	set, err := c.loadSet(setID)
	if err != nil || set.Artifact.ID != providerID {
		return Set{}, ErrInvalidSet
	}
	return set, nil
}

// CollectGarbage removes unreferenced sets and artifacts. Caller-protected
// sets represent generation, rollback, staged, and lease references. Desired
// config selections and installed artifacts are additional durable roots.
func (c *Catalog) CollectGarbage(protectedSetIDs []string) error {
	if c == nil {
		return ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkLayout(); err != nil {
		return err
	}
	state, err := c.readState()
	if err != nil {
		return err
	}
	protected := make(map[string]bool, len(protectedSetIDs)+len(state.DesiredSets))
	for _, id := range protectedSetIDs {
		if !validDigest(id) {
			return ErrInvalidConfig
		}
		protected[id] = true
	}
	for _, id := range state.DesiredSets {
		protected[id] = true
	}
	setRoot := filepath.Join(c.root, "sets")
	setItems, err := os.ReadDir(setRoot)
	if err != nil || len(setItems) > maxCatalogItems {
		return ErrUnsafeStore
	}
	type foundSet struct {
		id   string
		path string
		set  Set
		keep bool
	}
	sets := make([]foundSet, 0, len(setItems))
	known := make(map[string]bool, len(setItems))
	for _, item := range setItems {
		id := item.Name()
		if !validDigest(id) || item.Type()&os.ModeSymlink != 0 || !item.IsDir() {
			return ErrUnsafeStore
		}
		set, loadErr := c.loadSet(id)
		if loadErr != nil {
			return ErrUnsafeStore
		}
		known[id] = true
		sets = append(sets, foundSet{id: id, path: filepath.Join(setRoot, id), set: set, keep: protected[id]})
	}
	for id := range protected {
		if !known[id] {
			return ErrSetMissing
		}
	}
	artifactRoot := filepath.Join(c.root, "artifacts")
	artifactItems, err := os.ReadDir(artifactRoot)
	if err != nil || len(artifactItems) > maxCatalogItems {
		return ErrUnsafeStore
	}
	for _, item := range artifactItems {
		if !validDigest(item.Name()) || item.Type()&os.ModeSymlink != 0 || !item.IsDir() {
			return ErrUnsafeStore
		}
		if _, _, err := c.loadArtifact(item.Name()); err != nil {
			return ErrUnsafeStore
		}
	}
	for _, set := range sets {
		if !set.keep {
			if err := removeTreeOwned(set.path); err != nil {
				return ErrUnsafeStore
			}
		}
	}
	keepArtifacts := make(map[string]bool, len(state.Installed)+len(sets))
	for _, artifact := range state.Installed {
		keepArtifacts[artifact.Digest] = true
	}
	for _, set := range sets {
		if set.keep {
			keepArtifacts[set.set.Artifact.Digest] = true
		}
	}
	metadataPruned := false
	for digest := range state.ArtifactAttestations {
		if !keepArtifacts[digest] {
			delete(state.ArtifactAttestations, digest)
			metadataPruned = true
		}
	}
	// Persist metadata pruning before deleting the now-unreferenced bytes. A
	// crash can leave collectible artifact residue, but cannot leave a trusted
	// selection record pointing at bytes that GC already removed.
	if metadataPruned {
		if err := c.writeState(state); err != nil {
			return err
		}
	}
	for _, item := range artifactItems {
		if keepArtifacts[item.Name()] {
			continue
		}
		path := filepath.Join(artifactRoot, item.Name())
		if _, _, err := c.loadArtifact(item.Name()); err != nil {
			return ErrUnsafeStore
		}
		if err := removeTreeOwned(path); err != nil {
			return ErrUnsafeStore
		}
	}
	if err := syncDirectory(setRoot); err != nil {
		return ErrUnsafeStore
	}
	if err := syncDirectory(artifactRoot); err != nil {
		return ErrUnsafeStore
	}
	return nil
}

// StartProvider launches exactly the immutable binary pinned by setID,
// configures it from the private snapshot, and requires a successful probe.
func (c *Catalog) StartProvider(ctx context.Context, setID, socketPath, tokenPath string) (*storageproto.Client, error) {
	if c == nil || ctx == nil || !validDigest(setID) || !filepath.IsAbs(socketPath) || !filepath.IsAbs(tokenPath) {
		return nil, ErrInvalidConfig
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	set, err := c.loadSet(setID)
	if err != nil {
		return nil, err
	}
	binaryPath := filepath.Join(c.root, "artifacts", set.Artifact.Digest, "provider")
	startCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	client, err := storageproto.Start(startCtx, storageproto.StartOptions{Binary: binaryPath, SocketPath: socketPath, TokenFile: tokenPath, StartupTimeout: probeTimeout})
	if err != nil {
		cancel()
		return nil, err
	}
	descriptor, err := client.Describe(startCtx)
	if err == nil {
		var fingerprint string
		fingerprint, err = storageproto.Fingerprint(descriptor)
		if err == nil && (descriptor.ID != set.Artifact.ID || descriptor.Version != set.Artifact.Version || descriptor.ProtocolVersion != set.Artifact.ProtocolVersion || fingerprint != set.Artifact.DescriptorFingerprint) {
			err = ErrInvalidArtifact
		}
	}
	if err == nil {
		err = client.Configure(startCtx, toProtocolConfig(set.Config))
	}
	if err == nil {
		err = client.Probe(startCtx)
	}
	cancel()
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

func (c *Catalog) loadSet(id string) (Set, error) {
	if !validDigest(id) || c.checkLayout() != nil {
		return Set{}, ErrInvalidSet
	}
	dir := filepath.Join(c.root, "sets", id)
	if err := requireDirectory(dir, 0700); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Set{}, ErrSetMissing
		}
		return Set{}, ErrInvalidSet
	}
	if !directoryHasNames(dir, []string{"config.json", "manifest.json"}) {
		return Set{}, ErrInvalidSet
	}
	manifestBytes, err := readPrivateRegular(filepath.Join(dir, "manifest.json"), maxManifestBytes, 0400)
	if err != nil {
		return Set{}, ErrInvalidSet
	}
	var stored setManifest
	if decodeStrict(manifestBytes, &stored) != nil || stored.SchemaVersion != SchemaVersion || !validDigest(stored.ConfigSHA256) {
		return Set{}, ErrInvalidSet
	}
	if stored.Attestation != nil && stored.Attestation.Validate() != nil {
		return Set{}, ErrInvalidSet
	}
	canonicalManifest, err := encodeSetManifest(stored.Artifact, stored.ConfigSHA256, stored.Attestation)
	if err != nil || !bytes.Equal(manifestBytes, canonicalManifest) || digestBytes(manifestBytes) != id {
		return Set{}, ErrInvalidSet
	}
	artifact, descriptor, err := c.loadArtifact(stored.Artifact.Digest)
	if err != nil || artifact != stored.Artifact {
		return Set{}, ErrInvalidSet
	}
	configBytes, err := readPrivateRegular(filepath.Join(dir, "config.json"), storageproto.MaxConfigBytes, 0600)
	if err != nil {
		return Set{}, ErrInvalidSet
	}
	configSum := sha256.Sum256(configBytes)
	if hex.EncodeToString(configSum[:]) != stored.ConfigSHA256 {
		return Set{}, ErrInvalidSet
	}
	var config SetConfig
	if decodeStrict(configBytes, &config) != nil {
		return Set{}, ErrInvalidSet
	}
	canonical, encoded, err := canonicalConfig(config)
	if err != nil || !bytes.Equal(configBytes, encoded) || int64(len(encoded)) > storageproto.MaxConfigBytes ||
		storageproto.ValidateConfigForSchema(descriptor.ConfigurationSchema, toProtocolConfig(canonical)) != nil {
		return Set{}, ErrInvalidSet
	}
	var attestation *plugintrust.Attestation
	if stored.Attestation != nil {
		copy := *stored.Attestation
		attestation = &copy
	}
	return Set{ID: id, Artifact: artifact, Config: canonical, Attestation: attestation}, nil
}

func (c *Catalog) loadArtifact(digest string) (Artifact, storageproto.Descriptor, error) {
	if !validDigest(digest) || c.checkLayout() != nil {
		return Artifact{}, storageproto.Descriptor{}, ErrInvalidArtifact
	}
	dir := filepath.Join(c.root, "artifacts", digest)
	if err := requireDirectory(dir, 0500); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Artifact{}, storageproto.Descriptor{}, ErrArtifactMissing
		}
		return Artifact{}, storageproto.Descriptor{}, ErrInvalidArtifact
	}
	if !directoryHasNames(dir, []string{"descriptor.json", "provider"}) {
		return Artifact{}, storageproto.Descriptor{}, ErrInvalidArtifact
	}
	binaryPath := filepath.Join(dir, "provider")
	info, err := os.Lstat(binaryPath)
	if err != nil || info.Size() <= 0 || info.Size() > MaxArtifactBytes || info.Mode().Perm() != 0500 || info.Mode()&os.ModeSymlink != 0 {
		return Artifact{}, storageproto.Descriptor{}, ErrInvalidArtifact
	}
	if err := verifyExecutable(binaryPath, info.Size(), digest); err != nil {
		return Artifact{}, storageproto.Descriptor{}, ErrInvalidArtifact
	}
	descriptorBytes, err := readPrivateRegular(filepath.Join(dir, "descriptor.json"), storageproto.MaxControlFrameBytes, 0400)
	if err != nil {
		return Artifact{}, storageproto.Descriptor{}, ErrInvalidArtifact
	}
	var descriptor storageproto.Descriptor
	if decodeStrict(descriptorBytes, &descriptor) != nil {
		return Artifact{}, storageproto.Descriptor{}, ErrInvalidArtifact
	}
	canonicalDescriptor, err := json.Marshal(descriptor)
	if err != nil || !bytes.Equal(descriptorBytes, canonicalDescriptor) {
		return Artifact{}, storageproto.Descriptor{}, ErrInvalidArtifact
	}
	fingerprint, err := storageproto.Fingerprint(descriptor)
	if err != nil {
		return Artifact{}, storageproto.Descriptor{}, ErrInvalidArtifact
	}
	artifact := Artifact{Digest: digest, ID: descriptor.ID, Version: descriptor.Version, ProtocolVersion: descriptor.ProtocolVersion, DescriptorFingerprint: fingerprint, Size: info.Size()}
	return artifact, descriptor, nil
}

func (c *Catalog) publishArtifact(stageDir string, artifact Artifact, descriptor storageproto.Descriptor) error {
	if err := writePrivateFile(filepath.Join(stageDir, "descriptor.json"), mustJSON(descriptor), 0400); err != nil {
		return fmt.Errorf("%w: write artifact descriptor: %v", ErrUnsafeStore, err)
	}
	// Keep the staging directory writable until it has been renamed: some
	// filesystems reject renaming a read-only source directory. The published
	// name is locked down immediately after the atomic rename.
	if err := os.Chmod(stageDir, 0700); err != nil {
		return fmt.Errorf("%w: secure artifact stage: %v", ErrUnsafeStore, err)
	}
	if err := syncDirectory(stageDir); err != nil {
		return fmt.Errorf("%w: sync artifact stage: %v", ErrUnsafeStore, err)
	}
	artifactRoot := filepath.Join(c.root, "artifacts")
	target := filepath.Join(artifactRoot, artifact.Digest)
	if _, err := os.Lstat(target); err == nil {
		stored, _, loadErr := c.loadArtifact(artifact.Digest)
		if loadErr != nil || stored != artifact {
			return ErrUnsafeStore
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrUnsafeStore
	}
	if err := os.Rename(stageDir, target); err != nil {
		if stored, _, loadErr := c.loadArtifact(artifact.Digest); loadErr == nil && stored == artifact {
			return nil
		}
		return fmt.Errorf("%w: publish artifact directory: %v", ErrUnsafeStore, err)
	}
	if err := os.Chmod(target, 0500); err != nil {
		return fmt.Errorf("%w: secure published artifact: %v", ErrUnsafeStore, err)
	}
	if err := syncDirectory(artifactRoot); err != nil {
		return fmt.Errorf("%w: sync artifact catalog: %v", ErrUnsafeStore, err)
	}
	return nil
}

func (c *Catalog) ensureLayout() error {
	if err := ensurePrivateDirectory(c.root, 0700); err != nil {
		return ErrUnsafeStore
	}
	for _, name := range []string{"artifacts", "sets", "staging"} {
		if err := ensurePrivateDirectory(filepath.Join(c.root, name), 0700); err != nil {
			return ErrUnsafeStore
		}
	}
	return nil
}

func (c *Catalog) checkLayout() error {
	for _, path := range []string{c.root, filepath.Join(c.root, "artifacts"), filepath.Join(c.root, "sets"), filepath.Join(c.root, "staging")} {
		if err := requireDirectory(path, 0700); err != nil {
			return ErrUnsafeStore
		}
	}
	return nil
}

func (c *Catalog) cleanStaging() error {
	if err := c.checkLayout(); err != nil {
		return err
	}
	stageRoot := filepath.Join(c.root, "staging")
	items, err := os.ReadDir(stageRoot)
	if err != nil || len(items) > maxCatalogItems {
		return ErrUnsafeStore
	}
	for _, item := range items {
		if err := removeTreeOwned(filepath.Join(stageRoot, item.Name())); err != nil {
			return ErrUnsafeStore
		}
	}
	return syncDirectory(stageRoot)
}

func (c *Catalog) readState() (catalogState, error) {
	if c.checkLayout() != nil {
		return catalogState{}, ErrUnsafeStore
	}
	data, err := readPrivateRegular(filepath.Join(c.root, "state.json"), maxStateBytes, 0600)
	if err != nil {
		return catalogState{}, ErrUnsafeStore
	}
	var state catalogState
	if decodeStrict(data, &state) != nil || state.SchemaVersion != SchemaVersion || state.Installed == nil || state.DesiredSets == nil || len(state.Installed) > maxCatalogItems || len(state.DesiredSets) > maxCatalogItems || len(state.ArtifactAttestations) > maxCatalogItems || len(state.InstalledAttestations) > maxCatalogItems {
		return catalogState{}, ErrUnsafeStore
	}
	canonical, err := encodeState(state)
	if err != nil || !bytes.Equal(data, canonical) {
		return catalogState{}, ErrUnsafeStore
	}
	lastID := ""
	for _, artifact := range state.Installed {
		if !validArtifact(artifact) || artifact.ID <= lastID {
			return catalogState{}, ErrUnsafeStore
		}
		lastID = artifact.ID
		stored, _, loadErr := c.loadArtifact(artifact.Digest)
		if loadErr != nil || stored != artifact {
			return catalogState{}, ErrUnsafeStore
		}
	}
	for digest, attestations := range state.ArtifactAttestations {
		if !validDigest(digest) || len(attestations) == 0 || len(attestations) > maxArtifactAttestations {
			return catalogState{}, ErrUnsafeStore
		}
		for index, attestation := range attestations {
			if attestation.Validate() != nil || index > 0 && !attestationLess(attestations[index-1], attestation) {
				return catalogState{}, ErrUnsafeStore
			}
		}
	}
	for providerID, attestation := range state.InstalledAttestations {
		if !validProviderID(providerID) || !hasInstalledID(state.Installed, providerID) || attestation.Validate() != nil {
			return catalogState{}, ErrUnsafeStore
		}
	}
	for providerID, setID := range state.DesiredSets {
		if !validProviderID(providerID) || !validDigest(setID) {
			return catalogState{}, ErrUnsafeStore
		}
		set, loadErr := c.loadSet(setID)
		if loadErr != nil || set.Artifact.ID != providerID || !hasInstalledID(state.Installed, providerID) {
			return catalogState{}, ErrUnsafeStore
		}
	}
	return state, nil
}

func (c *Catalog) writeState(state catalogState) error {
	state.SchemaVersion = SchemaVersion
	if state.Installed == nil {
		state.Installed = []Artifact{}
	}
	if state.DesiredSets == nil {
		state.DesiredSets = map[string]string{}
	}
	sort.Slice(state.Installed, func(i, j int) bool { return state.Installed[i].ID < state.Installed[j].ID })
	data, err := encodeState(state)
	if err != nil || len(data) > maxStateBytes {
		return ErrUnsafeStore
	}
	stage, err := os.CreateTemp(filepath.Join(c.root, "staging"), "state-")
	if err != nil {
		return ErrUnsafeStore
	}
	stagePath := stage.Name()
	defer os.Remove(stagePath)
	if err := stage.Chmod(0600); err != nil {
		_ = stage.Close()
		return ErrUnsafeStore
	}
	_, writeErr := stage.Write(data)
	if writeErr == nil {
		writeErr = stage.Sync()
	}
	closeErr := stage.Close()
	if writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return ErrUnsafeStore
	}
	if err := syncDirectory(filepath.Dir(stagePath)); err != nil {
		return ErrUnsafeStore
	}
	if err := os.Rename(stagePath, filepath.Join(c.root, "state.json")); err != nil {
		return ErrUnsafeStore
	}
	if err := syncDirectory(c.root); err != nil {
		return ErrUnsafeStore
	}
	return nil
}

func encodeState(state catalogState) ([]byte, error) {
	copy := catalogState{
		SchemaVersion: SchemaVersion, Installed: append([]Artifact{}, state.Installed...),
		DesiredSets: make(map[string]string, len(state.DesiredSets)),
	}
	sort.Slice(copy.Installed, func(i, j int) bool { return copy.Installed[i].ID < copy.Installed[j].ID })
	for id, setID := range state.DesiredSets {
		copy.DesiredSets[id] = setID
	}
	if len(state.ArtifactAttestations) > 0 {
		copy.ArtifactAttestations = make(map[string][]plugintrust.Attestation, len(state.ArtifactAttestations))
		for digest, attestations := range state.ArtifactAttestations {
			ordered := append([]plugintrust.Attestation(nil), attestations...)
			sort.Slice(ordered, func(i, j int) bool { return attestationLess(ordered[i], ordered[j]) })
			copy.ArtifactAttestations[digest] = ordered
		}
	}
	if len(state.InstalledAttestations) > 0 {
		copy.InstalledAttestations = make(map[string]plugintrust.Attestation, len(state.InstalledAttestations))
		for providerID, attestation := range state.InstalledAttestations {
			copy.InstalledAttestations[providerID] = attestation
		}
	}
	return json.Marshal(copy)
}

func encodeSetManifest(artifact Artifact, configSHA string, attestation *plugintrust.Attestation) ([]byte, error) {
	if !validArtifact(artifact) || !validDigest(configSHA) {
		return nil, ErrInvalidSet
	}
	if attestation != nil && attestation.Validate() != nil {
		return nil, ErrInvalidSet
	}
	return json.Marshal(setManifest{SchemaVersion: SchemaVersion, Artifact: artifact, ConfigSHA256: configSHA, Attestation: attestation})
}

func canonicalConfig(config SetConfig) (SetConfig, []byte, error) {
	protocolConfig := toProtocolConfig(config)
	if storageproto.ValidateConfig(protocolConfig) != nil {
		return SetConfig{}, nil, ErrInvalidConfig
	}
	canonical := SetConfig{Values: make(map[string]json.RawMessage, len(config.Values)), Secrets: make(map[string]string, len(config.Secrets))}
	for key, raw := range config.Values {
		value, err := canonicalJSON(raw)
		if err != nil {
			return SetConfig{}, nil, ErrInvalidConfig
		}
		canonical.Values[key] = value
	}
	for key, value := range config.Secrets {
		canonical.Secrets[key] = value
	}
	encoded, err := json.Marshal(canonical)
	if err != nil || len(encoded) > storageproto.MaxConfigBytes {
		return SetConfig{}, nil, ErrInvalidConfig
	}
	return canonical, encoded, nil
}

func canonicalJSON(raw json.RawMessage) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, ErrInvalidConfig
	}
	encoded, err := json.Marshal(value)
	return json.RawMessage(encoded), err
}

func toProtocolConfig(config SetConfig) storageproto.Config {
	return storageproto.Config{Values: cloneRawMap(config.Values), Secrets: cloneStringMap(config.Secrets)}
}

func cloneConfig(config SetConfig) SetConfig {
	return SetConfig{Values: cloneRawMap(config.Values), Secrets: cloneStringMap(config.Secrets)}
}

func cloneRawMap(values map[string]json.RawMessage) map[string]json.RawMessage {
	copy := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		copy[key] = append(json.RawMessage(nil), value...)
	}
	return copy
}

func cloneStringMap(values map[string]string) map[string]string {
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}

func replaceArtifact(artifacts []Artifact, next Artifact) []Artifact {
	out := make([]Artifact, 0, len(artifacts)+1)
	replaced := false
	for _, artifact := range artifacts {
		if artifact.ID == next.ID {
			if !replaced {
				out = append(out, next)
				replaced = true
			}
			continue
		}
		out = append(out, artifact)
	}
	if !replaced {
		out = append(out, next)
	}
	return out
}

func hasInstalledID(artifacts []Artifact, id string) bool {
	for _, artifact := range artifacts {
		if artifact.ID == id {
			return true
		}
	}
	return false
}

func installedArtifact(artifacts []Artifact, id string) (Artifact, bool) {
	for _, artifact := range artifacts {
		if artifact.ID == id {
			return artifact, true
		}
	}
	return Artifact{}, false
}

func rememberArtifactAttestation(state *catalogState, digest string, attestation plugintrust.Attestation) error {
	if state == nil || !validDigest(digest) || attestation.Validate() != nil {
		return ErrInvalidConfig
	}
	if state.ArtifactAttestations == nil {
		state.ArtifactAttestations = make(map[string][]plugintrust.Attestation)
	}
	attestations := state.ArtifactAttestations[digest]
	for _, existing := range attestations {
		if existing == attestation {
			return nil
		}
	}
	if len(attestations) >= maxArtifactAttestations {
		return ErrInvalidConfig
	}
	attestations = append(attestations, attestation)
	sort.Slice(attestations, func(i, j int) bool { return attestationLess(attestations[i], attestations[j]) })
	state.ArtifactAttestations[digest] = attestations
	return nil
}

func attestationLess(left, right plugintrust.Attestation) bool {
	leftKey := string(left.Provenance) + "\x00" + string(left.Authority) + "\x00" + string(left.Publisher) + "\x00" + fmt.Sprint(left.Reviewed)
	rightKey := string(right.Provenance) + "\x00" + string(right.Authority) + "\x00" + string(right.Publisher) + "\x00" + fmt.Sprint(right.Reviewed)
	return leftKey < rightKey
}

func sameSetContent(a, b Set) bool {
	if a.ID != b.ID || a.Artifact != b.Artifact || !sameAttestation(a.Attestation, b.Attestation) || len(a.Config.Values) != len(b.Config.Values) || len(a.Config.Secrets) != len(b.Config.Secrets) {
		return false
	}
	for key, value := range a.Config.Values {
		if !bytes.Equal(value, b.Config.Values[key]) {
			return false
		}
	}
	for key, value := range a.Config.Secrets {
		if value != b.Config.Secrets[key] {
			return false
		}
	}
	return true
}

func sameAttestation(a, b *plugintrust.Attestation) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func validArtifact(artifact Artifact) bool {
	return validDigest(artifact.Digest) && validProviderID(artifact.ID) && artifact.Version != "" &&
		artifact.ProtocolVersion == storageproto.Version && validDigest(artifact.DescriptorFingerprint) && artifact.Size > 0 && artifact.Size <= MaxArtifactBytes
}

func validProviderID(id string) bool {
	if len(id) < 1 || len(id) > 63 || id[0] < 'a' || id[0] > 'z' {
		return false
	}
	for _, value := range id[1:] {
		if !(value >= 'a' && value <= 'z' || value >= '0' && value <= '9' || value == '-') {
			return false
		}
	}
	return true
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func digestBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func decodeStrict(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing JSON data")
	}
	return nil
}

func mustJSON(value any) []byte {
	data, _ := json.Marshal(value)
	return data
}

func createProbeCredentials(dir string) (string, string, error) {
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return "", "", err
	}
	tokenPath := filepath.Join(dir, "token")
	if err := writePrivateFile(tokenPath, []byte(hex.EncodeToString(secret[:])), 0600); err != nil {
		return "", "", err
	}
	for i := range secret {
		secret[i] = 0
	}
	return filepath.Join(dir, "provider.sock"), tokenPath, nil
}

func copyContext(ctx context.Context, dst io.Writer, src io.Reader, buffer []byte, limit int64) (int64, error) {
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, readErr := src.Read(buffer)
		if n > 0 {
			if total+int64(n) > limit {
				return total, errors.New("payload limit exceeded")
			}
			written, err := dst.Write(buffer[:n])
			total += int64(written)
			if err != nil {
				return total, err
			}
			if written != n {
				return total, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return total, nil
			}
			return total, readErr
		}
	}
}
