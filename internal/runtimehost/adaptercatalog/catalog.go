// Package adaptercatalog imports trusted-local adapter executables into
// immutable Runtime Host-owned artifacts and adapter-set snapshots.
//
// It deliberately knows nothing about platform semantics. Descriptors are
// obtained and validated by the same adapterhost implementation used by the
// application processes.
package adaptercatalog

import (
	"bytes"
	"context"
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
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/adapterhost"
	"github.com/integrated-recorder/core/internal/adapterproto"
)

const (
	SchemaVersion       = 1
	BinaryPrefix        = "integrated-recorder-adapter-"
	MaxArtifactBytes    = 512 << 20
	maxManifestBytes    = 1 << 20
	maxAdapters         = 256
	maxSourceDirs       = 32
	maxSourceDirEntries = 4096
	maxRejectedCodes    = 16
	quarantineCapacity  = 1024
	probeTimeout        = 6 * time.Second
	copyBufferSize      = 256 << 10
)

var (
	ErrInvalidConfig = errors.New("adapter catalog configuration is invalid")
	ErrUnsafeStore   = errors.New("adapter catalog storage is unsafe")
	ErrSetNotFound   = errors.New("adapter set is unavailable")
	ErrInvalidSet    = errors.New("adapter set is invalid")
)

// Snapshot is a bounded projection of an immutable adapter set. Directory is
// the private set bin directory intended for ADAPTER_DIR; it is never suitable
// for public API output.
type Snapshot struct {
	ID            string   `json:"id"`
	Directory     string   `json:"-"`
	Entries       []Entry  `json:"entries"`
	RejectedCount int      `json:"rejected_count,omitempty"`
	RejectedCodes []string `json:"rejected_codes,omitempty"`
}

// Entry identifies both the adapter's validated protocol identity and the
// immutable executable bytes used by this set.
type Entry struct {
	AdapterID             string `json:"adapter_id"`
	Version               string `json:"version"`
	ProtocolVersion       int    `json:"protocol_version"`
	DescriptorFingerprint string `json:"descriptor_fingerprint"`
	ArtifactSHA256        string `json:"artifact_sha256"`
	ArtifactSize          int64  `json:"artifact_size"`
	BinaryName            string `json:"binary_name"`
}

type manifest struct {
	SchemaVersion int     `json:"schema_version"`
	Entries       []Entry `json:"entries"`
}

type candidate struct {
	path  string
	entry Entry
}

type rejection struct {
	code      string
	adapterID string
}

type Catalog struct {
	root            string
	sources         []string
	gate            chan struct{}
	quarantine      map[string]rejection
	quarantineOrder []string
	qMu             sync.Mutex
	// afterSourceCopy is an internal deterministic race-test seam. Production
	// callers leave it nil; it never participates in catalog identity.
	afterSourceCopy func(string)
}

// Open creates the private catalog layout. root and source directories must
// be absolute, clean paths so callers cannot accidentally make identity
// depend on a process working directory.
func Open(root string, sourceDirs []string) (*Catalog, error) {
	if !validAbsoluteClean(root) || filepath.Clean(root) == string(filepath.Separator) || len(sourceDirs) > maxSourceDirs {
		return nil, ErrInvalidConfig
	}
	ordered := append([]string(nil), sourceDirs...)
	for _, dir := range ordered {
		if !validAbsoluteClean(dir) || filepath.Clean(dir) == string(filepath.Separator) {
			return nil, ErrInvalidConfig
		}
	}
	sort.Strings(ordered)
	ordered = compactStrings(ordered)
	c := &Catalog{root: filepath.Clean(root), sources: ordered, gate: make(chan struct{}, 1), quarantine: map[string]rejection{}}
	if err := c.ensureLayout(); err != nil {
		return nil, err
	}
	return c, nil
}

func validAbsoluteClean(path string) bool {
	return path != "" && filepath.IsAbs(path) && filepath.Clean(path) == path
}

func compactStrings(values []string) []string {
	if len(values) == 0 {
		return values
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

func (c *Catalog) acquire(ctx context.Context) error {
	select {
	case c.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Catalog) release() { <-c.gate }

// Reconcile serializes directory scans, imports stable executable snapshots,
// validates each with the production Core adapterhost, and publishes one
// immutable set. Candidate failures are isolated: valid independent adapters
// can still be published. A fallback entry is retained only when a rejected
// or ambiguous source could have replaced that exact binary/adapter identity.
func (c *Catalog) Reconcile(ctx context.Context, fallbackSetID string) (Snapshot, error) {
	return c.ReconcileWithSources(ctx, fallbackSetID, nil)
}

// ReconcileWithSources imports the catalog's configured trusted-local source
// directories plus the immutable Host-owned source snapshots supplied for
// this operation. Additional sources let Runtime Host add verified remote
// registry artifacts without mutating the catalog's configured source list.
// Callers must serialize source selection with application update operations.
func (c *Catalog) ReconcileWithSources(ctx context.Context, fallbackSetID string, additionalSources []string) (Snapshot, error) {
	if c == nil || ctx == nil {
		return Snapshot{}, ErrInvalidConfig
	}
	if len(additionalSources) > maxSourceDirs || len(c.sources)+len(additionalSources) > maxSourceDirs {
		return Snapshot{}, ErrInvalidConfig
	}
	sources := append([]string(nil), c.sources...)
	for _, source := range additionalSources {
		if !validAbsoluteClean(source) || filepath.Clean(source) == string(filepath.Separator) {
			return Snapshot{}, ErrInvalidConfig
		}
		sources = append(sources, source)
	}
	sort.Strings(sources)
	sources = compactStrings(sources)
	if err := c.acquire(ctx); err != nil {
		return Snapshot{}, err
	}
	defer c.release()
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err := c.checkLayout(); err != nil {
		return Snapshot{}, err
	}
	var fallback Snapshot
	var err error
	if fallbackSetID != "" {
		fallback, err = c.load(fallbackSetID)
		if err != nil {
			return Snapshot{}, ErrInvalidSet
		}
	}

	paths, overflow, err := c.sourceCandidates(sources)
	if err != nil {
		return c.fallbackOrError(fallback, fallbackSetID, "inventory_unavailable", err)
	}
	if overflow {
		return c.fallbackOrError(fallback, fallbackSetID, "inventory_limit_exceeded", ErrUnsafeStore)
	}
	rejectedByPath := make(map[string]string)
	failedNames := make(map[string]bool)
	failedIDs := make(map[string]bool)

	// Duplicate source basenames across directories are ambiguous even if one
	// copy happens to validate. Reject the whole group before launching either
	// executable so source-directory ordering never selects a winner.
	pathsByName := make(map[string][]string)
	for _, path := range paths {
		pathsByName[filepath.Base(path)] = append(pathsByName[filepath.Base(path)], path)
	}
	duplicateNames := make([]string, 0)
	for name, group := range pathsByName {
		if len(group) > 1 {
			duplicateNames = append(duplicateNames, name)
		}
	}
	sort.Strings(duplicateNames)
	for _, name := range duplicateNames {
		failedNames[name] = true
		for _, path := range pathsByName[name] {
			rejectedByPath[path] = "duplicate_binary_name"
		}
	}

	valid := make([]candidate, 0, len(paths))
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return Snapshot{}, err
		}
		if rejectedByPath[path] != "" {
			continue
		}
		item, code := c.importCandidate(ctx, path, fallback.Entries)
		if code != "" {
			if code == "catalog_unavailable" || code == "artifact_publish_failed" {
				return c.fallbackOrError(fallback, fallbackSetID, code, ErrUnsafeStore)
			}
			rejectedByPath[path] = code
			failedNames[filepath.Base(path)] = true
			if item.entry.AdapterID != "" {
				failedIDs[item.entry.AdapterID] = true
			}
			continue
		}
		valid = append(valid, item)
	}

	// Reject every member of an ambiguous descriptor ID group;
	// choosing the first source would make directory ordering an activation
	// policy and could silently replace a plugin.
	byID := map[string][]int{}
	for i, item := range valid {
		byID[item.entry.AdapterID] = append(byID[item.entry.AdapterID], i)
	}
	duplicateIDs := make([]string, 0)
	for id, indexes := range byID {
		if len(indexes) > 1 {
			duplicateIDs = append(duplicateIDs, id)
		}
	}
	sort.Strings(duplicateIDs)
	remove := make(map[int]bool)
	for _, id := range duplicateIDs {
		failedIDs[id] = true
		for _, index := range byID[id] {
			remove[index] = true
			item := valid[index]
			rejectedByPath[item.path] = "duplicate_adapter_id"
			failedNames[item.entry.BinaryName] = true
		}
	}
	filtered := valid[:0]
	for i, item := range valid {
		if remove[i] {
			continue
		}
		filtered = append(filtered, item)
	}
	valid = filtered

	// Preserve fallback entries only where a failed source could represent that
	// exact prior binary, or a duplicate descriptor ID makes replacement
	// ambiguous. If that preservation conflicts with another candidate, reject
	// the candidate deterministically and repeat until the retained set is
	// conflict-free.
	preserved := map[string]Entry{}
	for changed := true; changed; {
		changed = false
		preserved = make(map[string]Entry)
		for _, entry := range fallback.Entries {
			if failedNames[entry.BinaryName] || failedIDs[entry.AdapterID] {
				preserved[entry.AdapterID] = entry
			}
		}
		filtered = valid[:0]
		for _, item := range valid {
			conflictCode := ""
			for _, old := range preserved {
				if item.entry.AdapterID == old.AdapterID {
					conflictCode = "duplicate_adapter_id"
					failedIDs[old.AdapterID] = true
					break
				}
				if item.entry.BinaryName == old.BinaryName {
					conflictCode = "duplicate_binary_name"
					failedNames[old.BinaryName] = true
					break
				}
			}
			if conflictCode != "" {
				rejectedByPath[item.path] = conflictCode
				failedNames[item.entry.BinaryName] = true
				changed = true
				continue
			}
			filtered = append(filtered, item)
		}
		valid = filtered
	}
	entries := make([]Entry, 0, len(valid)+len(preserved))
	for _, entry := range preserved {
		entries = append(entries, entry)
	}
	for _, item := range valid {
		entries = append(entries, item.entry)
	}
	sortEntries(entries)
	snapshot, err := c.publishSet(entries)
	if err != nil {
		return c.fallbackOrError(fallback, fallbackSetID, "set_publish_failed", err)
	}
	snapshot.RejectedCount, snapshot.RejectedCodes = rejectionSummary(rejectedByPath)
	return snapshot, nil
}

func (c *Catalog) fallbackOrError(fallback Snapshot, fallbackSetID, code string, cause error) (Snapshot, error) {
	if fallbackSetID != "" {
		fallback.RejectedCount = 1
		fallback.RejectedCodes = []string{code}
		return fallback, nil
	}
	if errors.Is(cause, ErrUnsafeStore) {
		return Snapshot{}, ErrUnsafeStore
	}
	return Snapshot{}, cause
}

func rejectionSummary(byPath map[string]string) (int, []string) {
	codes := make([]string, 0, len(byPath))
	seen := make(map[string]bool)
	for _, code := range byPath {
		if code != "" && !seen[code] {
			seen[code] = true
			codes = append(codes, code)
		}
	}
	sort.Strings(codes)
	if len(codes) > maxRejectedCodes {
		codes = codes[:maxRejectedCodes]
	}
	return len(byPath), codes
}

// Empty loads or publishes the canonical empty adapter set.
func (c *Catalog) Empty() (Snapshot, error) {
	if c == nil {
		return Snapshot{}, ErrInvalidConfig
	}
	if err := c.acquire(context.Background()); err != nil {
		return Snapshot{}, err
	}
	defer c.release()
	return c.publishSet([]Entry{})
}

// Load validates the content identity, manifest, immutable artifact objects,
// and executable snapshot of a durable adapter set.
func (c *Catalog) Load(id string) (Snapshot, error) {
	if c == nil {
		return Snapshot{}, ErrInvalidConfig
	}
	if err := c.acquire(context.Background()); err != nil {
		return Snapshot{}, err
	}
	defer c.release()
	return c.load(id)
}

func (c *Catalog) load(id string) (Snapshot, error) {
	if !validDigest(id) || c.checkLayout() != nil {
		return Snapshot{}, ErrInvalidSet
	}
	setDir := filepath.Join(c.root, "sets", id)
	if err := requireDirectory(setDir, false); err != nil {
		return Snapshot{}, ErrSetNotFound
	}
	if !directoryHasNames(setDir, []string{"adapter-set.json", "bin"}) {
		return Snapshot{}, ErrInvalidSet
	}
	manifestPath := filepath.Join(setDir, "adapter-set.json")
	data, err := readPrivateRegular(manifestPath, maxManifestBytes)
	if err != nil {
		return Snapshot{}, ErrInvalidSet
	}
	var stored manifest
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&stored) != nil || decoder.Decode(new(any)) != io.EOF || stored.SchemaVersion != SchemaVersion || len(stored.Entries) > maxAdapters {
		return Snapshot{}, ErrInvalidSet
	}
	canonical, err := encodeManifest(stored.Entries)
	if err != nil || !bytes.Equal(data, canonical) || digest(data) != id {
		return Snapshot{}, ErrInvalidSet
	}
	if !entriesCanonical(stored.Entries) {
		return Snapshot{}, ErrInvalidSet
	}
	binDir := filepath.Join(setDir, "bin")
	if err := requireDirectory(binDir, false); err != nil {
		return Snapshot{}, ErrInvalidSet
	}
	binNames := make([]string, 0, len(stored.Entries))
	for _, entry := range stored.Entries {
		binNames = append(binNames, entry.BinaryName)
	}
	sort.Strings(binNames)
	if !directoryHasNames(binDir, binNames) {
		return Snapshot{}, ErrInvalidSet
	}
	for _, entry := range stored.Entries {
		if !validEntry(entry) {
			return Snapshot{}, ErrInvalidSet
		}
		artifactDir := filepath.Join(c.root, "artifacts", entry.ArtifactSHA256)
		if err := verifyArtifactDirectory(artifactDir, entry.ArtifactSHA256); err != nil {
			return Snapshot{}, ErrInvalidSet
		}
		binaryPath := filepath.Join(binDir, entry.BinaryName)
		if err := verifyExecutable(binaryPath, entry.ArtifactSize, entry.ArtifactSHA256); err != nil {
			return Snapshot{}, ErrInvalidSet
		}
	}
	return Snapshot{ID: id, Directory: binDir, Entries: cloneEntries(stored.Entries)}, nil
}

// Collect removes only verified sets and artifacts which are not referenced
// by keepIDs. Any malformed or unexpected store entry makes collection fail
// closed without deleting data.
func (c *Catalog) Collect(keepIDs []string) error {
	if c == nil {
		return ErrInvalidConfig
	}
	if err := c.acquire(context.Background()); err != nil {
		return err
	}
	defer c.release()
	if err := c.checkLayout(); err != nil {
		return err
	}
	keep := make(map[string]bool, len(keepIDs))
	for _, id := range keepIDs {
		if !validDigest(id) {
			return ErrInvalidConfig
		}
		keep[id] = true
	}
	setRoot := filepath.Join(c.root, "sets")
	items, err := os.ReadDir(setRoot)
	if err != nil {
		return ErrUnsafeStore
	}
	type setItem struct {
		id, path string
		entries  []Entry
		keep     bool
	}
	sets := make([]setItem, 0, len(items))
	for _, item := range items {
		id := item.Name()
		if !validDigest(id) || !item.IsDir() {
			return ErrUnsafeStore
		}
		path := filepath.Join(setRoot, id)
		snapshot, loadErr := c.load(id)
		if loadErr != nil {
			return ErrUnsafeStore
		}
		sets = append(sets, setItem{id: id, path: path, entries: snapshot.Entries, keep: keep[id]})
	}
	knownSets := make(map[string]bool, len(sets))
	for _, set := range sets {
		knownSets[set.id] = true
	}
	for id := range keep {
		if !knownSets[id] {
			return ErrUnsafeStore
		}
	}
	artifactRoot := filepath.Join(c.root, "artifacts")
	artifactItems, err := os.ReadDir(artifactRoot)
	if err != nil {
		return ErrUnsafeStore
	}
	for _, item := range artifactItems {
		if !validDigest(item.Name()) || !item.IsDir() {
			return ErrUnsafeStore
		}
		if err := verifyArtifactDirectory(filepath.Join(artifactRoot, item.Name()), item.Name()); err != nil {
			return ErrUnsafeStore
		}
	}
	for _, set := range sets {
		if set.keep {
			continue
		}
		if err := removeTreeOwned(set.path); err != nil {
			return ErrUnsafeStore
		}
	}
	references := map[string]bool{}
	for _, set := range sets {
		if set.keep {
			for _, entry := range set.entries {
				references[entry.ArtifactSHA256] = true
			}
		}
	}
	for _, item := range artifactItems {
		if references[item.Name()] {
			continue
		}
		path := filepath.Join(artifactRoot, item.Name())
		if err := verifyArtifactDirectory(path, item.Name()); err != nil {
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

func (c *Catalog) sourceCandidates(sourceDirs []string) ([]string, bool, error) {
	paths := []string{}
	overflow := false
	for _, dir := range sourceDirs {
		info, err := os.Lstat(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, false, ErrUnsafeStore
		}
		file, err := openSourceNoFollow(dir)
		if err != nil {
			return nil, false, ErrUnsafeStore
		}
		opened, err := file.Stat()
		if err != nil || !opened.IsDir() || !os.SameFile(info, opened) {
			_ = file.Close()
			return nil, false, ErrUnsafeStore
		}
		items := make([]os.DirEntry, 0, 64)
		for len(items) <= maxSourceDirEntries {
			batch, readErr := file.ReadDir(maxSourceDirEntries + 1 - len(items))
			items = append(items, batch...)
			if len(items) > maxSourceDirEntries {
				overflow = true
				break
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				_ = file.Close()
				return nil, false, ErrUnsafeStore
			}
			if len(batch) == 0 {
				break
			}
		}
		if err := file.Close(); err != nil {
			return nil, false, ErrUnsafeStore
		}
		after, err := os.Lstat(dir)
		if err != nil || !after.IsDir() || after.Mode()&os.ModeSymlink != 0 || !os.SameFile(opened, after) {
			return nil, false, ErrUnsafeStore
		}
		if overflow {
			break
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Name() < items[j].Name() })
		for _, item := range items {
			name := item.Name()
			if !strings.HasPrefix(name, BinaryPrefix) {
				continue
			}
			paths = append(paths, filepath.Join(dir, name))
			if len(paths) > maxAdapters {
				overflow = true
				paths = paths[:maxAdapters]
				break
			}
		}
		if overflow {
			break
		}
	}
	sort.Strings(paths)
	return paths, overflow, nil
}

func (c *Catalog) importCandidate(ctx context.Context, source string, fallback []Entry) (candidate, string) {
	name := filepath.Base(source)
	parent, parentErr := os.Lstat(filepath.Dir(source))
	if !safeBinaryName(name) || parentErr != nil || !parent.IsDir() || parent.Mode()&os.ModeSymlink != 0 {
		return candidate{}, "unsafe_candidate"
	}
	before, err := os.Lstat(source)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Mode().Perm()&0111 == 0 {
		return candidate{}, "invalid_executable"
	}
	if before.Size() <= 0 || before.Size() > MaxArtifactBytes {
		return candidate{}, "artifact_size_invalid"
	}
	input, err := openSourceNoFollow(source)
	if err != nil {
		return candidate{}, "invalid_executable"
	}
	defer input.Close()
	opened, err := input.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) || opened.Size() != before.Size() || !opened.ModTime().Equal(before.ModTime()) {
		return candidate{}, "source_unstable"
	}
	if err := ctx.Err(); err != nil {
		return candidate{}, "source_unstable"
	}
	if err := ensurePrivateDirectory(filepath.Join(c.root, "staging")); err != nil {
		return candidate{}, "catalog_unavailable"
	}
	stageDir, err := os.MkdirTemp(filepath.Join(c.root, "staging"), "adapter-")
	if err != nil {
		return candidate{}, "catalog_unavailable"
	}
	defer removeTreeOwned(stageDir)
	stagePath := filepath.Join(stageDir, "adapter")
	output, err := os.OpenFile(stagePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return candidate{}, "catalog_unavailable"
	}
	h := sha256.New()
	buffer := make([]byte, copyBufferSize)
	written, copyErr := copyContext(ctx, io.MultiWriter(output, h), input, buffer, MaxArtifactBytes)
	if copyErr == nil && written != before.Size() {
		copyErr = errors.New("size changed")
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
			return candidate{}, "source_unstable"
		}
		return candidate{}, "source_unstable"
	}
	digestHex := hex.EncodeToString(h.Sum(nil))
	if c.afterSourceCopy != nil {
		c.afterSourceCopy(source)
	}
	afterPath, statErr := os.Lstat(source)
	afterFD, fdErr := input.Stat()
	parentAfter, parentStatErr := os.Lstat(filepath.Dir(source))
	if statErr != nil || fdErr != nil || parentStatErr != nil || parentAfter.Mode()&os.ModeSymlink != 0 || !parentAfter.IsDir() || !os.SameFile(parent, parentAfter) || afterPath.Mode()&os.ModeSymlink != 0 || !afterPath.Mode().IsRegular() || !os.SameFile(before, afterPath) || !os.SameFile(opened, afterFD) || before.Size() != afterPath.Size() || !before.ModTime().Equal(afterPath.ModTime()) || opened.Size() != afterFD.Size() || !opened.ModTime().Equal(afterFD.ModTime()) {
		c.rememberRejected(digestHex, "source_unstable")
		return candidate{}, "source_unstable"
	}
	if rejected, ok := c.quarantined(digestHex); ok {
		return candidate{path: source, entry: Entry{AdapterID: rejected.adapterID, BinaryName: name}}, rejected.code
	}
	if err := os.Chmod(stagePath, 0500); err != nil {
		return candidate{}, "catalog_unavailable"
	}
	if err := syncDirectory(stageDir); err != nil {
		return candidate{}, "catalog_unavailable"
	}
	probeDir, err := os.MkdirTemp(filepath.Join(c.root, "staging"), "probe-")
	if err != nil {
		return candidate{}, "catalog_unavailable"
	}
	defer removeTreeOwned(probeDir)
	binDir := filepath.Join(probeDir, "bin")
	if err := os.Mkdir(binDir, 0700); err != nil {
		return candidate{}, "catalog_unavailable"
	}
	probePath := filepath.Join(binDir, name)
	if err := copyFile(stagePath, probePath, 0500); err != nil {
		return candidate{}, "catalog_unavailable"
	}
	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	adapterHost, err := adapterhost.DiscoverDirs(probeCtx, []string{binDir}, nil)
	if err != nil {
		c.rememberRejected(digestHex, "probe_failed")
		return candidate{}, "probe_failed"
	}
	list := adapterHost.List()
	var descriptor *adapterproto.Descriptor
	readyCount := 0
	for _, item := range list {
		if item.Status.State == "ready" && item.Descriptor != nil {
			readyCount++
			copy := *item.Descriptor
			descriptor = &copy
		}
	}
	// Core's Host.Close owns bounded process shutdown and kills an adapter
	// which does not honor the protocol shutdown request.
	adapterHost.Close()
	if readyCount != 1 || descriptor == nil {
		c.rememberRejected(digestHex, "descriptor_rejected")
		return candidate{}, "descriptor_rejected"
	}
	if descriptor.ProtocolVersion != adapterproto.Version {
		c.rememberRejected(digestHex, "unsupported_protocol")
		return candidate{}, "unsupported_protocol"
	}
	if err := descriptor.Validate(); err != nil {
		c.rememberRejected(digestHex, "descriptor_rejected")
		return candidate{}, "descriptor_rejected"
	}
	entry := Entry{
		AdapterID:             descriptor.ID,
		Version:               descriptor.Version,
		ProtocolVersion:       descriptor.ProtocolVersion,
		DescriptorFingerprint: adapterhost.DescriptorFingerprint(*descriptor),
		ArtifactSHA256:        digestHex,
		ArtifactSize:          written,
		BinaryName:            name,
	}
	if !validEntry(entry) {
		c.rememberRejected(digestHex, "descriptor_rejected")
		return candidate{}, "descriptor_rejected"
	}
	for _, previous := range fallback {
		if previous.AdapterID != entry.AdapterID || previous.Version != entry.Version {
			continue
		}
		if previous.DescriptorFingerprint != entry.DescriptorFingerprint || previous.ArtifactSHA256 != entry.ArtifactSHA256 {
			// An adapter ID/version pair is an immutable identity claim. A
			// different descriptor or executable under the same claim is
			// ambiguous, so retain the known-good entry and quarantine these bytes.
			c.rememberRejectedIdentity(digestHex, "identity_conflict", entry.AdapterID)
			return candidate{path: source, entry: entry}, "identity_conflict"
		}
	}
	if err := c.publishArtifact(stageDir, stagePath, digestHex, written); err != nil {
		return candidate{}, "artifact_publish_failed"
	}
	return candidate{path: source, entry: entry}, ""
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

func (c *Catalog) publishArtifact(stageDir, stagePath, sha string, size int64) error {
	artifactRoot := filepath.Join(c.root, "artifacts")
	if err := ensurePrivateDirectory(artifactRoot); err != nil {
		return ErrUnsafeStore
	}
	target := filepath.Join(artifactRoot, sha)
	if _, err := os.Lstat(target); err == nil {
		if verifyExecutable(filepath.Join(target, "adapter"), size, sha) != nil {
			return ErrUnsafeStore
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrUnsafeStore
	}
	if err := os.Chmod(stageDir, 0700); err != nil {
		return ErrUnsafeStore
	}
	if err := os.Chmod(stagePath, 0500); err != nil {
		return ErrUnsafeStore
	}
	if err := os.Rename(stageDir, target); err != nil {
		if _, statErr := os.Lstat(target); statErr == nil && verifyExecutable(filepath.Join(target, "adapter"), size, sha) == nil {
			return nil
		}
		return ErrUnsafeStore
	}
	if err := syncDirectory(artifactRoot); err != nil {
		return ErrUnsafeStore
	}
	return nil
}

func (c *Catalog) publishSet(entries []Entry) (Snapshot, error) {
	if len(entries) > maxAdapters {
		return Snapshot{}, ErrInvalidSet
	}
	entries = cloneEntries(entries)
	sortEntries(entries)
	if !entriesCanonical(entries) {
		return Snapshot{}, ErrInvalidSet
	}
	manifestBytes, err := encodeManifest(entries)
	if err != nil || len(manifestBytes) > maxManifestBytes {
		return Snapshot{}, ErrInvalidSet
	}
	id := digest(manifestBytes)
	setRoot := filepath.Join(c.root, "sets")
	if err := ensurePrivateDirectory(setRoot); err != nil {
		return Snapshot{}, fmt.Errorf("%w: prepare adapter sets", ErrUnsafeStore)
	}
	target := filepath.Join(setRoot, id)
	if _, err := os.Lstat(target); err == nil {
		return c.load(id)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, fmt.Errorf("%w: inspect adapter set target", ErrUnsafeStore)
	}
	stageDir, err := os.MkdirTemp(filepath.Join(c.root, "staging"), "set-")
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: stage adapter set", ErrUnsafeStore)
	}
	defer removeTreeOwned(stageDir)
	binDir := filepath.Join(stageDir, "bin")
	if err := os.Mkdir(binDir, 0700); err != nil {
		return Snapshot{}, fmt.Errorf("%w: stage adapter binaries", ErrUnsafeStore)
	}
	for _, entry := range entries {
		if !validEntry(entry) {
			return Snapshot{}, ErrInvalidSet
		}
		artifact := filepath.Join(c.root, "artifacts", entry.ArtifactSHA256, "adapter")
		if err := verifyExecutable(artifact, entry.ArtifactSize, entry.ArtifactSHA256); err != nil {
			return Snapshot{}, ErrInvalidSet
		}
		if err := copyFile(artifact, filepath.Join(binDir, entry.BinaryName), 0500); err != nil {
			return Snapshot{}, fmt.Errorf("%w: stage adapter binary", ErrUnsafeStore)
		}
	}
	manifestPath := filepath.Join(stageDir, "adapter-set.json")
	f, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: create adapter set manifest", ErrUnsafeStore)
	}
	_, writeErr := f.Write(manifestBytes)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	closeErr := f.Close()
	if writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		return Snapshot{}, fmt.Errorf("%w: write adapter set manifest", ErrUnsafeStore)
	}
	if err := os.Chmod(manifestPath, 0400); err != nil {
		return Snapshot{}, fmt.Errorf("%w: secure adapter set manifest", ErrUnsafeStore)
	}
	if err := os.Chmod(binDir, 0500); err != nil {
		return Snapshot{}, fmt.Errorf("%w: secure adapter set binaries", ErrUnsafeStore)
	}
	if err := syncDirectory(binDir); err != nil {
		return Snapshot{}, fmt.Errorf("%w: sync adapter set binaries", ErrUnsafeStore)
	}
	if err := syncDirectory(stageDir); err != nil {
		return Snapshot{}, fmt.Errorf("%w: sync adapter set manifest", ErrUnsafeStore)
	}
	if err := os.Rename(stageDir, target); err != nil {
		if _, statErr := os.Lstat(target); statErr == nil {
			return c.load(id)
		}
		return Snapshot{}, fmt.Errorf("%w: publish adapter set directory", ErrUnsafeStore)
	}
	if err := os.Chmod(target, 0500); err != nil {
		return Snapshot{}, fmt.Errorf("%w: secure published adapter set", ErrUnsafeStore)
	}
	if err := syncDirectory(setRoot); err != nil {
		return Snapshot{}, fmt.Errorf("%w: publish adapter set", ErrUnsafeStore)
	}
	return Snapshot{ID: id, Directory: filepath.Join(target, "bin"), Entries: entries}, nil
}

func encodeManifest(entries []Entry) ([]byte, error) {
	copy := cloneEntries(entries)
	sortEntries(copy)
	return json.Marshal(manifest{SchemaVersion: SchemaVersion, Entries: copy})
}

func sortEntries(entries []Entry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].AdapterID != entries[j].AdapterID {
			return entries[i].AdapterID < entries[j].AdapterID
		}
		return entries[i].BinaryName < entries[j].BinaryName
	})
}

func entriesCanonical(entries []Entry) bool {
	if len(entries) > maxAdapters {
		return false
	}
	seenID, seenName := map[string]bool{}, map[string]bool{}
	for i, entry := range entries {
		if !validEntry(entry) || seenID[entry.AdapterID] || seenName[entry.BinaryName] {
			return false
		}
		if i > 0 {
			prev := entries[i-1]
			if prev.AdapterID > entry.AdapterID || (prev.AdapterID == entry.AdapterID && prev.BinaryName >= entry.BinaryName) {
				return false
			}
		}
		seenID[entry.AdapterID], seenName[entry.BinaryName] = true, true
	}
	return true
}

func validEntry(entry Entry) bool {
	return adapterproto.IsValidIdentifier(entry.AdapterID) && entry.Version != "" && len(entry.Version) <= 128 && entry.ProtocolVersion == adapterproto.Version && validDigest(entry.DescriptorFingerprint) && validDigest(entry.ArtifactSHA256) && entry.ArtifactSize > 0 && entry.ArtifactSize <= MaxArtifactBytes && safeBinaryName(entry.BinaryName)
}

func safeBinaryName(name string) bool {
	return name != "" && len(name) <= 255 && filepath.Base(name) == name && !strings.ContainsAny(name, `/\\`) && !strings.ContainsRune(name, 0) && strings.HasPrefix(name, BinaryPrefix) && name != BinaryPrefix
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && strings.ToLower(value) == value
}

func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func cloneEntries(in []Entry) []Entry {
	out := make([]Entry, len(in))
	copy(out, in)
	return out
}

func (c *Catalog) rememberRejected(identity, code string) {
	c.rememberRejectedIdentity(identity, code, "")
}

func (c *Catalog) rememberRejectedIdentity(identity, code, adapterID string) {
	if !validDigest(identity) {
		return
	}
	c.qMu.Lock()
	defer c.qMu.Unlock()
	if previous, ok := c.quarantine[identity]; ok {
		if previous.adapterID == "" && adapterID != "" {
			previous.adapterID = adapterID
			c.quarantine[identity] = previous
		}
		return
	}
	if len(c.quarantineOrder) >= quarantineCapacity {
		old := c.quarantineOrder[0]
		c.quarantineOrder = c.quarantineOrder[1:]
		delete(c.quarantine, old)
	}
	c.quarantine[identity] = rejection{code: code, adapterID: adapterID}
	c.quarantineOrder = append(c.quarantineOrder, identity)
}

func (c *Catalog) quarantined(identity string) (rejection, bool) {
	c.qMu.Lock()
	defer c.qMu.Unlock()
	result, ok := c.quarantine[identity]
	return result, ok
}

func (c *Catalog) String() string {
	return fmt.Sprintf("adapter catalog (%d source directories)", len(c.sources))
}
