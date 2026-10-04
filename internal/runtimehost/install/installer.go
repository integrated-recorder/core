// Package install stages verified, immutable application releases. It never
// executes an artifact and never extracts an archive.
package install

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/integrated-recorder/core/internal/runtimehost/release"
)

const (
	maxSignatureBytes = 1024
	artifactMode      = 0500
	metadataMode      = 0400
	directoryMode     = 0700
)

var (
	ErrInvalidConfig   = errors.New("invalid release installer configuration")
	ErrInvalidSource   = errors.New("invalid release source")
	ErrSourceRead      = errors.New("release source could not be read")
	ErrUnsafeDirectory = errors.New("unsafe release installation directory")
	ErrReleaseExists   = errors.New("release is already installed")
	ErrArtifactWrite   = errors.New("release artifact could not be staged")
	ErrStagePublish    = errors.New("verified release could not be published")
)

// Source provides the exact signed manifest bytes and readers for the artifact
// filenames named by that manifest. Implementations must honor ctx while
// opening remote resources. Installer still enforces all byte limits.
type Source interface {
	Manifest(ctx context.Context) (manifestJSON []byte, signature string, err error)
	OpenArtifact(ctx context.Context, filename string) (io.ReadCloser, error)
}

// ReleaseNotesSummarySource optionally provides a bounded plain-text summary
// for display. It is untrusted presentation text and must never be rendered as
// HTML or used to construct navigation targets.
type ReleaseNotesSummarySource interface {
	ReleaseNotesSummary() string
}

// LocalDirectorySource reads an offline release bundle containing release.json,
// release.json.sig, and each artifact at the directory root.
type LocalDirectorySource struct {
	Directory string
}

func (s LocalDirectorySource) Manifest(ctx context.Context) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	root, err := resolveSourceRoot(s.Directory)
	if err != nil {
		return nil, "", err
	}
	manifest, err := readBoundedRegular(filepath.Join(root, "release.json"), release.MaxManifestBytes)
	if err != nil {
		return nil, "", ErrSourceRead
	}
	signature, err := readBoundedRegular(filepath.Join(root, "release.json.sig"), maxSignatureBytes)
	if err != nil {
		return nil, "", ErrSourceRead
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	return manifest, string(signature), nil
}

func (s LocalDirectorySource) OpenArtifact(ctx context.Context, filename string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !safeFilename(filename) {
		return nil, ErrInvalidSource
	}
	root, err := resolveSourceRoot(s.Directory)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(root, filename)
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, ErrSourceRead
	}
	f, err := openNoFollow(path)
	if err != nil {
		return nil, ErrSourceRead
	}
	after, err := f.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
		_ = f.Close()
		return nil, ErrSourceRead
	}
	return f, nil
}

// StagedRelease contains only safe release identity and verification state.
// It intentionally contains no installation path or process information.
type StagedRelease struct {
	ID            string `json:"id"`
	Version       string `json:"version"`
	Commit        string `json:"commit"`
	Channel       string `json:"channel"`
	Verified      bool   `json:"verified"`
	ArtifactCount int    `json:"artifact_count"`
}

// Installer verifies release trust and compatibility before writing artifacts
// into a private, randomly named staging directory under runtimeRoot.
type Installer struct {
	runtimeRoot   string
	trustedKeys   map[string]ed25519.PublicKey
	compatibility release.HostCompatibility
	stageGate     chan struct{}
}

func NewInstaller(runtimeRoot string, trustedKeys map[string]ed25519.PublicKey, compatibility release.HostCompatibility) (*Installer, error) {
	if strings.TrimSpace(runtimeRoot) == "" {
		return nil, ErrInvalidConfig
	}
	abs, err := filepath.Abs(runtimeRoot)
	if err != nil || filepath.Clean(abs) == string(filepath.Separator) {
		return nil, ErrInvalidConfig
	}
	keys := make(map[string]ed25519.PublicKey, len(trustedKeys))
	for id, key := range trustedKeys {
		copyKey := append(ed25519.PublicKey(nil), key...)
		keys[id] = copyKey
	}
	return &Installer{runtimeRoot: filepath.Clean(abs), trustedKeys: keys, compatibility: compatibility, stageGate: make(chan struct{}, 1)}, nil
}

// Stage verifies then atomically publishes one immutable release. Existing
// generation directories are never replaced. No executable is started.
func (i *Installer) Stage(ctx context.Context, source Source) (StagedRelease, error) {
	if i == nil || ctx == nil || source == nil || i.stageGate == nil {
		return StagedRelease{}, ErrInvalidConfig
	}
	select {
	case i.stageGate <- struct{}{}:
		defer func() { <-i.stageGate }()
	case <-ctx.Done():
		return StagedRelease{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return StagedRelease{}, err
	}
	manifestBytes, signature, err := source.Manifest(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return StagedRelease{}, ctx.Err()
		}
		return StagedRelease{}, ErrSourceRead
	}
	if len(manifestBytes) == 0 || len(manifestBytes) > release.MaxManifestBytes || len(signature) == 0 || len(signature) > maxSignatureBytes {
		return StagedRelease{}, release.ErrInvalidManifest
	}
	manifestBytes = append([]byte(nil), manifestBytes...)
	manifest, err := release.DecodeAndVerifyManifest(manifestBytes, signature, i.trustedKeys)
	if err != nil {
		return StagedRelease{}, err
	}
	if err := release.CheckCompatibility(manifest, i.compatibility); err != nil {
		return StagedRelease{}, err
	}
	if err := ctx.Err(); err != nil {
		return StagedRelease{}, err
	}
	if err := i.ensureRoots(); err != nil {
		return StagedRelease{}, err
	}
	stagingRoot := filepath.Join(i.runtimeRoot, "staging")
	releaseRoot := filepath.Join(i.runtimeRoot, "releases")
	stageDir, err := os.MkdirTemp(stagingRoot, "stage-")
	if err != nil {
		return StagedRelease{}, ErrUnsafeDirectory
	}
	if err := os.Chmod(stageDir, directoryMode); err != nil {
		_ = os.RemoveAll(stageDir)
		return StagedRelease{}, ErrUnsafeDirectory
	}
	stageInfo, err := os.Lstat(stageDir)
	if err != nil || !stageInfo.IsDir() || stageInfo.Mode()&os.ModeSymlink != 0 {
		_ = os.RemoveAll(stageDir)
		return StagedRelease{}, ErrUnsafeDirectory
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		// This path was created by this invocation inside a private parent. Check
		// its identity before removing anything, and never follow a replacement.
		current, statErr := os.Lstat(stageDir)
		if statErr == nil && current.IsDir() && current.Mode()&os.ModeSymlink == 0 && os.SameFile(stageInfo, current) {
			_ = os.Chmod(stageDir, directoryMode)
			_ = os.RemoveAll(stageDir)
		}
	}()

	for _, artifact := range manifest.Artifacts {
		if err := ctx.Err(); err != nil {
			return StagedRelease{}, err
		}
		if !safeFilename(artifact.Filename) {
			return StagedRelease{}, release.ErrInvalidArtifact
		}
		body, openErr := source.OpenArtifact(ctx, artifact.Filename)
		if openErr != nil {
			if ctx.Err() != nil {
				return StagedRelease{}, ctx.Err()
			}
			return StagedRelease{}, ErrSourceRead
		}
		body = &closeOnceReadCloser{ReadCloser: body}
		writeErr := stageArtifact(ctx, stageDir, artifact, body)
		closeErr := body.Close()
		if writeErr != nil || closeErr != nil {
			if ctx.Err() != nil {
				return StagedRelease{}, ctx.Err()
			}
			if errors.Is(writeErr, release.ErrArtifactFile) {
				return StagedRelease{}, fmt.Errorf("%w: %w", ErrArtifactWrite, release.ErrArtifactFile)
			}
			return StagedRelease{}, ErrArtifactWrite
		}
	}
	if err := writeImmutableFile(stageDir, "release.json", manifestBytes, metadataMode); err != nil {
		return StagedRelease{}, ErrArtifactWrite
	}
	if err := writeImmutableFile(stageDir, "release.json.sig", []byte(signature), metadataMode); err != nil {
		return StagedRelease{}, ErrArtifactWrite
	}
	if err := ctx.Err(); err != nil {
		return StagedRelease{}, err
	}
	if err := syncDirectory(stageDir); err != nil {
		return StagedRelease{}, fmt.Errorf("%w: sync staged release", ErrStagePublish)
	}

	id := releaseID(manifest.ReleaseVersion, manifest.Commit)
	target := filepath.Join(releaseRoot, id)
	lock, err := acquirePublishLock(releaseRoot, id)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return StagedRelease{}, ErrReleaseExists
		}
		return StagedRelease{}, fmt.Errorf("%w: reserve release publication", ErrStagePublish)
	}
	defer func() {
		_ = lock.Close()
		_ = os.Remove(lock.Name())
		_ = syncDirectory(releaseRoot)
	}()
	if _, err := os.Lstat(target); err == nil {
		return StagedRelease{}, ErrReleaseExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return StagedRelease{}, ErrUnsafeDirectory
	}
	if err := ctx.Err(); err != nil {
		return StagedRelease{}, err
	}
	if err := os.Rename(stageDir, target); err != nil {
		var linkErr *os.LinkError
		if errors.As(err, &linkErr) {
			return StagedRelease{}, fmt.Errorf("%w: atomic release rename: %v", ErrStagePublish, linkErr.Err)
		}
		return StagedRelease{}, fmt.Errorf("%w: atomic release rename", ErrStagePublish)
	}
	committed = true
	if err := os.Chmod(target, 0500); err != nil {
		removePublishedTree(target, stageInfo)
		committed = false
		return StagedRelease{}, fmt.Errorf("%w: secure published release", ErrStagePublish)
	}
	if err := syncDirectory(target); err != nil {
		removePublishedTree(target, stageInfo)
		committed = false
		return StagedRelease{}, fmt.Errorf("%w: sync immutable release permissions", ErrStagePublish)
	}
	if err := syncDirectory(releaseRoot); err != nil {
		removePublishedTree(target, stageInfo)
		committed = false
		return StagedRelease{}, fmt.Errorf("%w: sync published release", ErrStagePublish)
	}
	return StagedRelease{ID: id, Version: manifest.ReleaseVersion, Commit: strings.ToLower(manifest.Commit), Channel: manifest.Channel, Verified: true, ArtifactCount: len(manifest.Artifacts)}, nil
}

func removePublishedTree(path string, original os.FileInfo) {
	current, err := os.Lstat(path)
	if err != nil || !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(original, current) {
		return
	}
	_ = os.Chmod(path, directoryMode)
	_ = os.RemoveAll(path)
}

func stageArtifact(ctx context.Context, stageDir string, artifact release.Artifact, source io.ReadCloser) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if artifact.Size < 1 || artifact.Size > release.MaxArtifactBytes || !safeFilename(artifact.Filename) {
		return release.ErrInvalidArtifact
	}
	destination := filepath.Join(stageDir, artifact.Filename)
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	copyErr := copyExactContext(ctx, out, source, artifact.Size)
	if copyErr == nil {
		copyErr = out.Sync()
	}
	if copyErr == nil {
		copyErr = out.Chmod(artifactMode)
	}
	if copyErr == nil {
		copyErr = out.Sync()
	}
	closeErr := out.Close()
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return copyErr
	}
	return release.VerifyArtifactFile(destination, artifact)
}

func copyExactContext(ctx context.Context, dst io.Writer, src io.ReadCloser, size int64) error {
	if size < 1 || size > release.MaxArtifactBytes {
		return release.ErrInvalidArtifact
	}
	done := make(chan struct{})
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		select {
		case <-ctx.Done():
			_ = src.Close()
		case <-done:
		}
	}()
	defer func() {
		close(done)
		<-closed
	}()
	limited := &contextReader{ctx: ctx, reader: src}
	written, err := io.CopyN(dst, limited, size)
	if err != nil || written != size {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return io.ErrUnexpectedEOF
	}
	var extra [1]byte
	n, err := limited.Read(extra[:])
	if n != 0 {
		return release.ErrArtifactFile
	}
	if err == nil {
		return release.ErrArtifactFile
	}
	if !errors.Is(err, io.EOF) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrSourceRead
	}
	return ctx.Err()
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

type closeOnceReadCloser struct {
	io.ReadCloser
	once sync.Once
	err  error
}

func (c *closeOnceReadCloser) Close() error {
	c.once.Do(func() { c.err = c.ReadCloser.Close() })
	return c.err
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func writeImmutableFile(directory, name string, data []byte, mode os.FileMode) error {
	if !safeFilename(name) || len(data) == 0 || len(data) > release.MaxManifestBytes {
		return release.ErrInvalidManifest
	}
	file, err := os.OpenFile(filepath.Join(directory, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if writeErr == nil {
		writeErr = file.Chmod(mode)
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr == nil {
		writeErr = closeErr
	}
	return writeErr
}

func (i *Installer) ensureRoots() error {
	root := i.runtimeRoot
	if err := ensurePrivateDir(root, true); err != nil {
		return err
	}
	for _, name := range []string{"staging", "releases"} {
		if err := ensurePrivateDir(filepath.Join(root, name), true); err != nil {
			return err
		}
	}
	if err := syncDirectory(root); err != nil {
		return ErrUnsafeDirectory
	}
	if err := syncDirectory(filepath.Dir(root)); err != nil {
		return ErrUnsafeDirectory
	}
	return nil
}

func ensurePrivateDir(path string, create bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) && create {
		if err := os.Mkdir(path, directoryMode); err != nil && !errors.Is(err, os.ErrExist) {
			return ErrUnsafeDirectory
		}
		info, err = os.Lstat(path)
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrUnsafeDirectory
	}
	if err := os.Chmod(path, directoryMode); err != nil {
		return ErrUnsafeDirectory
	}
	after, err := os.Lstat(path)
	if err != nil || !after.IsDir() || after.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, after) {
		return ErrUnsafeDirectory
	}
	return nil
}

func resolveSourceRoot(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", ErrInvalidSource
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", ErrInvalidSource
	}
	info, err := os.Lstat(filepath.Clean(abs))
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", ErrInvalidSource
	}
	return filepath.Clean(abs), nil
}

func readBoundedRegular(path string, limit int64) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Size() < 0 || before.Size() > limit {
		return nil, ErrSourceRead
	}
	f, err := openNoFollow(path)
	if err != nil {
		return nil, ErrSourceRead
	}
	defer f.Close()
	after, err := f.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) || after.Size() > limit {
		return nil, ErrSourceRead
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, ErrSourceRead
	}
	return data, nil
}

func acquirePublishLock(releaseRoot, id string) (*os.File, error) {
	name := filepath.Join(releaseRoot, "."+id+".lock")
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(name)
		return nil, err
	}
	return file, nil
}

func releaseID(version, commit string) string {
	hash := sha256.Sum256([]byte(version + "\x00" + strings.ToLower(commit)))
	return "release-" + hex.EncodeToString(hash[:])
}

func safeFilename(name string) bool {
	if name == "" || len(name) > 128 || name == "." || name == ".." || strings.Contains(name, "..") || strings.ContainsAny(name, `/\\:`) {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}
