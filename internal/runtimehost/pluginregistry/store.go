package pluginregistry

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/integrated-recorder/core/internal/plugintrust"
	"github.com/integrated-recorder/core/internal/runtimehost/adaptercatalog"
)

const (
	desiredSchemaVersion = 1
	maxDesiredBytes      = 1 << 20
	maxSnapshotBytes     = 1 << 20
)

type DesiredPlugin struct {
	ID           string                   `json:"id"`
	Name         string                   `json:"name"`
	Version      string                   `json:"version"`
	Channel      string                   `json:"channel"`
	Digest       string                   `json:"digest"`
	Filename     string                   `json:"filename"`
	Size         int64                    `json:"size"`
	SourceCommit string                   `json:"source_commit"`
	Attestation  *plugintrust.Attestation `json:"attestation,omitempty"`
}

type desiredState struct {
	SchemaVersion int             `json:"schema_version"`
	Revision      uint64          `json:"revision"`
	SourceSetID   string          `json:"source_set_id"`
	Plugins       []DesiredPlugin `json:"plugins"`
}

type sourceSetManifest struct {
	SchemaVersion int             `json:"schema_version"`
	SourceSetID   string          `json:"source_set_id"`
	Plugins       []DesiredPlugin `json:"plugins"`
}

func (m *Manager) ensureLayout() error {
	if !filepath.IsAbs(m.root) || filepath.Clean(m.root) != m.root || filepath.Clean(m.root) == string(filepath.Separator) {
		return ErrInvalidConfig
	}
	if err := ensurePrivateDirectory(m.root); err != nil {
		return ErrUnsafeStore
	}
	for _, name := range []string{"artifacts", "source-sets", "staging"} {
		if err := ensurePrivateDirectory(filepath.Join(m.root, name)); err != nil {
			return ErrUnsafeStore
		}
	}
	if err := cleanupStaging(filepath.Join(m.root, "staging")); err != nil {
		return ErrUnsafeStore
	}
	if err := cleanupAtomicTemps(m.root); err != nil {
		return ErrUnsafeStore
	}
	return nil
}

func cleanupAtomicTemps(root string) error {
	items, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, item := range items {
		if !strings.HasPrefix(item.Name(), ".pluginregistry-") || !strings.HasSuffix(item.Name(), ".tmp") {
			continue
		}
		if err := removeOwnedTree(filepath.Join(root, item.Name())); err != nil {
			return err
		}
	}
	return syncDirectory(root)
}

func ensurePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrUnsafeStore
	}
	if err := os.Chmod(path, 0700); err != nil {
		return err
	}
	return nil
}

// CollectGarbage removes only remote-registry source snapshots and cached
// artifacts that are not referenced by the current desired source set. Callers
// invoke it after adaptercatalog has synchronously imported the plan source.
// Adaptercatalog owns its own copies and generation/recording references.
func (m *Manager) CollectGarbage() error {
	if m == nil {
		return ErrInvalidConfig
	}
	m.gate <- struct{}{}
	defer m.release()
	state, err := m.loadDesired()
	if err != nil {
		return ErrUnsafeStore
	}
	if err := m.validateSourceSet(state.Plugins, state.SourceSetID); err != nil {
		return ErrUnsafeStore
	}
	setRoot := filepath.Join(m.root, "source-sets")
	sets, err := os.ReadDir(setRoot)
	if err != nil {
		return ErrUnsafeStore
	}
	for _, item := range sets {
		if item.Name() == state.SourceSetID {
			continue
		}
		if !validSetID(item.Name()) || item.Type()&os.ModeSymlink != 0 {
			return ErrUnsafeStore
		}
		if err := removeOwnedTree(filepath.Join(setRoot, item.Name())); err != nil {
			return ErrUnsafeStore
		}
	}
	if err := syncDirectory(setRoot); err != nil {
		return ErrUnsafeStore
	}
	keepArtifacts := make(map[string]bool, len(state.Plugins))
	for _, plugin := range state.Plugins {
		keepArtifacts[plugin.Digest] = true
	}
	artifactRoot := filepath.Join(m.root, "artifacts")
	artifacts, err := os.ReadDir(artifactRoot)
	if err != nil {
		return ErrUnsafeStore
	}
	for _, item := range artifacts {
		if !shaPattern.MatchString(item.Name()) || item.Type()&os.ModeSymlink != 0 {
			return ErrUnsafeStore
		}
		if keepArtifacts[item.Name()] {
			continue
		}
		path := filepath.Join(artifactRoot, item.Name())
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
			return ErrUnsafeStore
		}
		if err := os.Remove(path); err != nil {
			return ErrUnsafeStore
		}
	}
	if err := syncDirectory(artifactRoot); err != nil {
		return ErrUnsafeStore
	}
	if err := cleanupStaging(filepath.Join(m.root, "staging")); err != nil {
		return ErrUnsafeStore
	}
	return nil
}

func cleanupStaging(root string) error {
	items, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := removeOwnedTree(filepath.Join(root, item.Name())); err != nil {
			return err
		}
	}
	return syncDirectory(root)
}

func removeOwnedTree(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		return ErrUnsafeStore
	}
	if !info.IsDir() {
		if !info.Mode().IsRegular() {
			return ErrUnsafeStore
		}
		return os.Remove(path)
	}
	if err := os.Chmod(path, 0700); err != nil {
		return err
	}
	items, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := removeOwnedTree(filepath.Join(path, item.Name())); err != nil {
			return err
		}
	}
	return os.Remove(path)
}

func (m *Manager) loadDesired() (desiredState, error) {
	path := filepath.Join(m.root, "desired.json")
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		plugins := []DesiredPlugin{}
		setID, setErr := sourceSetID(plugins)
		if setErr != nil {
			return desiredState{}, ErrUnsafeStore
		}
		// A crash may have happened after atomically publishing this initial
		// snapshot but before publishing desired.json. With no durable desired
		// state, this exact empty snapshot cannot be referenced by a generation;
		// discard only an invalid residue and recreate it below.
		emptySet := filepath.Join(m.root, "source-sets", setID)
		if _, statErr := os.Lstat(emptySet); statErr == nil {
			if validateErr := m.validateSourceSet(plugins, setID); validateErr != nil {
				if removeErr := removeOwnedTree(emptySet); removeErr != nil {
					return desiredState{}, ErrUnsafeStore
				}
				if syncErr := syncDirectory(filepath.Join(m.root, "source-sets")); syncErr != nil {
					return desiredState{}, ErrUnsafeStore
				}
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return desiredState{}, ErrUnsafeStore
		}
		if _, err := m.ensureSourceSet(plugins, setID); err != nil {
			return desiredState{}, err
		}
		state := desiredState{SchemaVersion: desiredSchemaVersion, Revision: 1, SourceSetID: setID, Plugins: plugins}
		if err := m.writeDesired(state); err != nil {
			return desiredState{}, err
		}
		return state, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 || info.Size() < 0 || info.Size() > maxDesiredBytes {
		return desiredState{}, ErrUnsafeStore
	}
	file, err := openRegularNoFollow(path)
	if err != nil {
		return desiredState{}, ErrUnsafeStore
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) || opened.Size() != info.Size() {
		return desiredState{}, ErrUnsafeStore
	}
	data, err := io.ReadAll(io.LimitReader(file, maxDesiredBytes+1))
	if err != nil || int64(len(data)) != opened.Size() || int64(len(data)) > maxDesiredBytes || !utf8.Valid(data) {
		return desiredState{}, ErrUnsafeStore
	}
	if validateDesiredWireShape(data) != nil {
		return desiredState{}, ErrUnsafeStore
	}
	var state desiredState
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&state) != nil {
		return desiredState{}, ErrUnsafeStore
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) || validateDesired(state) != nil {
		return desiredState{}, ErrUnsafeStore
	}
	if err := m.validateSourceSet(state.Plugins, state.SourceSetID); err != nil {
		return desiredState{}, err
	}
	return state, nil
}

func validateDesiredWireShape(data []byte) error {
	root, err := strictJSONObject(data, "schema_version", "revision", "source_set_id", "plugins")
	if err != nil {
		return ErrUnsafeStore
	}
	pluginsRaw, err := strictJSONArray(root["plugins"], MaxPlugins)
	if err != nil {
		return ErrUnsafeStore
	}
	for _, plugin := range pluginsRaw {
		if _, err := strictJSONObjectOptional(plugin, []string{"id", "name", "version", "channel", "digest", "filename", "size", "source_commit"}, []string{"attestation"}); err != nil {
			return ErrUnsafeStore
		}
	}
	return nil
}

func validateDesired(state desiredState) error {
	if state.SchemaVersion != desiredSchemaVersion || state.Revision == 0 || len(state.Plugins) > MaxPlugins {
		return ErrUnsafeStore
	}
	if !sort.SliceIsSorted(state.Plugins, func(i, j int) bool { return state.Plugins[i].ID < state.Plugins[j].ID }) {
		return ErrUnsafeStore
	}
	seen := map[string]bool{}
	for _, plugin := range state.Plugins {
		if !validDesiredPlugin(plugin) || seen[plugin.ID] {
			return ErrUnsafeStore
		}
		seen[plugin.ID] = true
	}
	id, err := sourceSetID(state.Plugins)
	if err != nil || id != state.SourceSetID {
		return ErrUnsafeStore
	}
	return nil
}

func validDesiredPlugin(plugin DesiredPlugin) bool {
	return validPluginID(plugin.ID) && validText(plugin.Name, maxNameBytes) && validIdentity(plugin.Version) && plugin.Channel == "stable" && shaPattern.MatchString(plugin.Digest) && plugin.Filename == adaptercatalog.BinaryPrefix+plugin.ID && len(plugin.Filename) >= minFilenameBytes && validText(plugin.Filename, maxFilenameBytes) && plugin.Size > 0 && plugin.Size <= MaxArtifactBytes && validSourceCommit(plugin.SourceCommit) && (plugin.Attestation == nil || plugin.Attestation.Validate() == nil)
}

func (m *Manager) writeDesired(state desiredState) error {
	if err := validateDesired(state); err != nil {
		return ErrUnsafeStore
	}
	data, err := json.Marshal(state)
	if err != nil || len(data) > maxDesiredBytes {
		return ErrUnsafeStore
	}
	return writeAtomic(filepath.Join(m.root, "desired.json"), data, 0600)
}

func writeAtomic(path string, data []byte, mode os.FileMode) (retErr error) {
	dir := filepath.Dir(path)
	if err := ensurePrivateDirectory(dir); err != nil {
		return ErrUnsafeStore
	}
	if existing, err := os.Lstat(path); err == nil {
		if !existing.Mode().IsRegular() || existing.Mode()&os.ModeSymlink != 0 {
			return ErrUnsafeStore
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrUnsafeStore
	}
	tmp, err := os.CreateTemp(dir, ".pluginregistry-*.tmp")
	if err != nil {
		return ErrUnsafeStore
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		if retErr != nil {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(mode); err != nil {
		return ErrUnsafeStore
	}
	if _, err := tmp.Write(append(bytes.Clone(data), '\n')); err != nil {
		return ErrUnsafeStore
	}
	if err := tmp.Sync(); err != nil {
		return ErrUnsafeStore
	}
	if err := tmp.Close(); err != nil {
		return ErrUnsafeStore
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return ErrUnsafeStore
	}
	if err := syncDirectory(dir); err != nil {
		return ErrUnsafeStore
	}
	return nil
}

func sourceSetID(plugins []DesiredPlugin) (string, error) {
	canonical := cloneDesired(plugins)
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].ID < canonical[j].ID })
	data, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func cloneDesired(plugins []DesiredPlugin) []DesiredPlugin {
	out := append([]DesiredPlugin(nil), plugins...)
	if out == nil {
		out = []DesiredPlugin{}
	}
	for i := range out {
		if plugins[i].Attestation != nil {
			attestation := *plugins[i].Attestation
			out[i].Attestation = &attestation
		}
	}
	return out
}

// DesiredSources returns one explicitly classified source per immutable
// Registry binary. Exact file paths and AllowedIDs prevent a directory's
// unrelated executable from inheriting another plugin's Registry attestation.
func (m *Manager) DesiredSources() ([]adaptercatalog.Source, error) {
	if m == nil {
		return nil, ErrInvalidConfig
	}
	state, err := m.loadDesired()
	if err != nil {
		return nil, ErrUnsafeStore
	}
	binDir := safeSetDirectoryPath(m.root, state.SourceSetID)
	sources := make([]adaptercatalog.Source, 0, len(state.Plugins))
	for _, plugin := range state.Plugins {
		attestation := desiredAttestation(plugin)
		if attestation.Provenance == plugintrust.LegacyUnclassified {
			// Old desired selections did not record Registry authority. Keep
			// them conservative when admitting a new set.
			attestation = plugintrust.NewOperator()
		}
		sources = append(sources, adaptercatalog.Source{
			Path: filepath.Join(binDir, plugin.Filename), Attestation: attestation,
			AllowedIDs: []string{plugin.ID},
		})
	}
	return sources, nil
}

func (m *Manager) ensureSourceSet(plugins []DesiredPlugin, id string) (string, error) {
	if !shaPattern.MatchString(id) {
		return "", ErrUnsafeStore
	}
	target := filepath.Join(m.root, "source-sets", id)
	if _, err := os.Lstat(target); err == nil {
		if err := m.validateSourceSet(plugins, id); err != nil {
			return "", err
		}
		return filepath.Join(target, "bin"), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", ErrUnsafeStore
	}
	stage, err := os.MkdirTemp(filepath.Join(m.root, "staging"), "source-set-")
	if err != nil {
		return "", ErrInstallFailed
	}
	defer func() { _ = removeOwnedTree(stage) }()
	bin := filepath.Join(stage, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		return "", ErrInstallFailed
	}
	canonical := cloneDesired(plugins)
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].ID < canonical[j].ID })
	for _, plugin := range canonical {
		artifact := filepath.Join(m.root, "artifacts", plugin.Digest)
		if err := verifyFile(artifact, plugin.Size, plugin.Digest, true); err != nil {
			return "", ErrUnsafeStore
		}
		if err := copyVerified(artifact, filepath.Join(bin, plugin.Filename), plugin.Size, plugin.Digest); err != nil {
			return "", ErrInstallFailed
		}
	}
	manifest := sourceSetManifest{SchemaVersion: desiredSchemaVersion, SourceSetID: id, Plugins: canonical}
	data, err := json.Marshal(manifest)
	if err != nil || len(data) > maxSnapshotBytes {
		return "", ErrInstallFailed
	}
	manifestPath := filepath.Join(stage, "source-set.json")
	if err := writeExclusive(manifestPath, data, 0400); err != nil {
		return "", ErrInstallFailed
	}
	if err := os.Chmod(bin, 0500); err != nil {
		return "", ErrInstallFailed
	}
	if err := syncDirectory(bin); err != nil {
		return "", ErrInstallFailed
	}
	if err := syncDirectory(stage); err != nil {
		return "", ErrInstallFailed
	}
	if err := os.Rename(stage, target); err != nil {
		if _, statErr := os.Lstat(target); statErr == nil && m.validateSourceSet(plugins, id) == nil {
			return filepath.Join(target, "bin"), nil
		}
		return "", ErrInstallFailed
	}
	// On macOS and some hardened filesystems, renaming a non-writable source
	// directory is denied. Publish while the private staging root is writable,
	// then make the final snapshot immutable before returning it to the caller.
	// If the Host crashes between rename and chmod, desired.json still points at
	// the prior selection; startup validation rejects the unreferenced residue.
	if err := os.Chmod(target, 0500); err != nil {
		_ = removeOwnedTree(target)
		return "", ErrInstallFailed
	}
	if err := syncDirectory(target); err != nil {
		return "", ErrInstallFailed
	}
	if err := syncDirectory(filepath.Join(m.root, "source-sets")); err != nil {
		return "", ErrInstallFailed
	}
	return filepath.Join(target, "bin"), nil
}

func (m *Manager) validateSourceSet(expected []DesiredPlugin, id string) error {
	root := filepath.Join(m.root, "source-sets", id)
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0222 != 0 {
		return ErrUnsafeStore
	}
	bin := filepath.Join(root, "bin")
	binInfo, err := os.Lstat(bin)
	if err != nil || !binInfo.IsDir() || binInfo.Mode()&os.ModeSymlink != 0 || binInfo.Mode().Perm()&0222 != 0 {
		return ErrUnsafeStore
	}
	rootItems, err := os.ReadDir(root)
	if err != nil || len(rootItems) != 2 || rootItems[0].Name() != "bin" || rootItems[1].Name() != "source-set.json" || rootItems[0].Type()&os.ModeSymlink != 0 || rootItems[1].Type()&os.ModeSymlink != 0 {
		return ErrUnsafeStore
	}
	manifestPath := filepath.Join(root, "source-set.json")
	data, err := readPrivateRegular(manifestPath, maxSnapshotBytes, 0222)
	if err != nil || !utf8.Valid(data) {
		return ErrUnsafeStore
	}
	manifestPlugins, err := strictSourceSetManifestShape(data)
	if err != nil {
		return ErrUnsafeStore
	}
	var manifest sourceSetManifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&manifest) != nil {
		return ErrUnsafeStore
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) || manifest.SchemaVersion != desiredSchemaVersion || manifest.SourceSetID != id || len(manifest.Plugins) != len(manifestPlugins) {
		return ErrUnsafeStore
	}
	canonical := cloneDesired(expected)
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].ID < canonical[j].ID })
	if !equalDesired(canonical, manifest.Plugins) {
		return ErrUnsafeStore
	}
	items, err := os.ReadDir(bin)
	if err != nil || len(items) != len(canonical) {
		return ErrUnsafeStore
	}
	for i, plugin := range canonical {
		if items[i].Name() != plugin.Filename || items[i].Type()&os.ModeSymlink != 0 {
			return ErrUnsafeStore
		}
		if err := verifyFile(filepath.Join(bin, plugin.Filename), plugin.Size, plugin.Digest, true); err != nil {
			return ErrUnsafeStore
		}
	}
	return nil
}

func strictSourceSetManifestShape(data []byte) ([]json.RawMessage, error) {
	root, err := strictJSONObject(data, "schema_version", "source_set_id", "plugins")
	if err != nil {
		return nil, ErrUnsafeStore
	}
	plugins, err := strictJSONArray(root["plugins"], MaxPlugins)
	if err != nil {
		return nil, ErrUnsafeStore
	}
	for _, plugin := range plugins {
		if _, err := strictJSONObjectOptional(plugin, []string{"id", "name", "version", "channel", "digest", "filename", "size", "source_commit"}, []string{"attestation"}); err != nil {
			return nil, ErrUnsafeStore
		}
	}
	return plugins, nil
}

func readPrivateRegular(path string, max int64, deniedPerm os.FileMode) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Mode().Perm()&deniedPerm != 0 || before.Size() < 0 || before.Size() > max {
		return nil, ErrUnsafeStore
	}
	file, err := openRegularNoFollow(path)
	if err != nil {
		return nil, ErrUnsafeStore
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) || opened.Size() != before.Size() {
		return nil, ErrUnsafeStore
	}
	data, err := io.ReadAll(io.LimitReader(file, max+1))
	if err != nil || int64(len(data)) != opened.Size() || int64(len(data)) > max {
		return nil, ErrUnsafeStore
	}
	return data, nil
}

func writeExclusive(path string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Chmod(path, mode)
}

func copyVerified(source, target string, size int64, digest string) error {
	input, err := openRegularNoFollow(source)
	if err != nil {
		return ErrUnsafeStore
	}
	defer input.Close()
	before, err := input.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Size() != size {
		return ErrUnsafeStore
	}
	output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	h := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(output, h), io.LimitReader(input, size+1))
	if copyErr == nil && (written != size || hex.EncodeToString(h.Sum(nil)) != digest) {
		copyErr = ErrVerificationFailed
	}
	if copyErr == nil {
		copyErr = output.Sync()
	}
	closeErr := output.Close()
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return copyErr
	}
	if err := os.Chmod(target, 0500); err != nil {
		return err
	}
	return nil
}

func verifyFile(path string, size int64, digest string, executable bool) error {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Size() != size || size <= 0 || size > MaxArtifactBytes || before.Mode().Perm()&0077 != 0 {
		return ErrUnsafeStore
	}
	if executable && before.Mode().Perm() != 0500 || !executable && before.Mode().Perm()&0111 != 0 {
		return ErrUnsafeStore
	}
	f, err := openRegularNoFollow(path)
	if err != nil {
		return ErrUnsafeStore
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) || opened.Size() != size {
		return ErrUnsafeStore
	}
	h := sha256.New()
	count, err := io.Copy(h, io.LimitReader(f, size+1))
	if err != nil || count != size || hex.EncodeToString(h.Sum(nil)) != digest {
		return ErrUnsafeStore
	}
	after, err := os.Lstat(path)
	if err != nil || after.Mode()&os.ModeSymlink != 0 || !os.SameFile(opened, after) || after.Size() != size || !after.ModTime().Equal(opened.ModTime()) {
		return ErrUnsafeStore
	}
	return nil
}

func equalDesired(a, b []DesiredPlugin) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		left, right := a[i], b[i]
		if left.ID != right.ID || left.Name != right.Name || left.Version != right.Version || left.Channel != right.Channel || left.Digest != right.Digest || left.Filename != right.Filename || left.Size != right.Size || left.SourceCommit != right.SourceCommit || !samePluginAttestation(left.Attestation, right.Attestation) {
			return false
		}
	}
	return true
}

func samePluginAttestation(a, b *plugintrust.Attestation) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func strictJSONObjectOptional(data []byte, required, optional []string) (map[string]json.RawMessage, error) {
	fields, err := strictJSONObjectAny(data, len(required)+len(optional))
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]bool, len(required)+len(optional))
	for _, key := range required {
		allowed[key] = true
		if _, ok := fields[key]; !ok {
			return nil, ErrUnsafeStore
		}
	}
	for _, key := range optional {
		allowed[key] = true
	}
	for key := range fields {
		if !allowed[key] {
			return nil, ErrUnsafeStore
		}
	}
	return fields, nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func (m *Manager) artifactPath(digest string) string {
	return filepath.Join(m.root, "artifacts", digest)
}

func validSetID(id string) bool { return shaPattern.MatchString(id) }

func (m *Manager) persistVerifiedArtifact(stagePath, digest string, size int64) (string, error) {
	target := m.artifactPath(digest)
	if _, err := os.Lstat(target); err == nil {
		if verifyFile(target, size, digest, true) != nil {
			return "", ErrUnsafeStore
		}
		return target, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", ErrUnsafeStore
	}
	if err := os.Chmod(stagePath, 0500); err != nil {
		return "", ErrInstallFailed
	}
	if err := verifyFile(stagePath, size, digest, true); err != nil {
		return "", ErrVerificationFailed
	}
	if err := syncDirectory(filepath.Dir(stagePath)); err != nil {
		return "", ErrInstallFailed
	}
	// Link provides an atomic create-if-absent publication. A concurrent
	// publisher can never replace an already immutable digest path.
	if err := os.Link(stagePath, target); err != nil {
		if verifyFile(target, size, digest, true) == nil {
			return target, nil
		}
		return "", ErrInstallFailed
	}
	if err := syncDirectory(filepath.Dir(target)); err != nil {
		return "", ErrInstallFailed
	}
	return target, nil
}

func (m *Manager) ensureArtifact(plugin DesiredPlugin) error {
	path := m.artifactPath(plugin.Digest)
	if verifyFile(path, plugin.Size, plugin.Digest, true) != nil {
		return fmt.Errorf("%w", ErrUnsafeStore)
	}
	return nil
}

func safeSetDirectoryPath(root string, id string) string {
	if !validSetID(id) {
		return ""
	}
	return filepath.Join(root, "source-sets", id, "bin")
}
