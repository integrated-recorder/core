// Package storagelocal implements the Storage Provider Protocol v1 local
// filesystem provider. Recording objects remain in the existing recordings
// directory; non-recording keys use a private subtree below that directory.
package storagelocal

import (
	"container/list"
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

	"github.com/integrated-recorder/core/internal/buildinfo"
	"github.com/integrated-recorder/core/internal/storageproto"
)

const (
	providerID   = "local"
	providerName = "Local Storage"
	// Valid Protocol v1 keys reject controls, so this private directory cannot
	// collide with any recordings/ suffix mapped directly below the root.
	privateDirectory = "\x1f.ir-storage-local"
	privateObjects   = "other"
	temporaryPrefix  = "\x1f.ir-storage-local-tmp-"
	readDirBatchSize = 128
	// Keep the digest cache bounded. Cache entries are process-local hints only;
	// a new Provider hashes objects until it has observed their current version.
	maxDigestCacheEntries = 4096
	// Keep whole-provider walks aligned with Core's archive enumeration bound.
	maxListVisitedEntries = 5_000_000
	// Protocol keys are at most 1024 bytes: at most 512 one-byte components,
	// plus the provider-private namespace's two physical components.
	maxListTraversalDepth = 514
)

var (
	errUnsafeEntry = errors.New("local storage entry is not a safe regular file")
	errListLimit   = errors.New("local storage listing exceeds traversal limits")
)

// Provider implements the local filesystem storage provider.
type Provider struct {
	mu              sync.RWMutex
	root            string
	rootEpoch       uint64
	digests         map[string]*list.Element
	digestLRU       list.List
	digestCacheHits uint64
}

type digestCacheEntry struct {
	key     string
	version objectVersion
	digest  string
}

// New returns an unconfigured local storage provider.
func New() *Provider { return &Provider{} }

// Descriptor returns the stable protocol identity and Host-controlled root
// configuration field.
func (p *Provider) Descriptor() storageproto.Descriptor {
	maxRootLength := 4096
	return storageproto.Descriptor{
		ProtocolVersion: storageproto.Version,
		ID:              providerID,
		Name:            providerName,
		Version:         buildinfo.Current().Version,
		ConfigurationSchema: storageproto.Schema{Fields: []storageproto.Field{{
			Key:         "root",
			Control:     "text",
			Label:       "Archive root",
			Description: "Host-controlled directory for recording archive objects.",
			Required:    true,
			Constraints: &storageproto.Constraints{MaxLength: &maxRootLength},
		}}},
		Capabilities: []string{
			storageproto.CapabilityRead,
			storageproto.CapabilityWrite,
			storageproto.CapabilityStat,
			storageproto.CapabilityList,
			storageproto.CapabilityDelete,
			storageproto.CapabilityRangeRead,
			storageproto.CapabilityAtomicReplace,
		},
	}
}

// Configure accepts only the required Host-supplied root field.
func (p *Provider) Configure(ctx context.Context, config storageproto.Config) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := storageproto.ValidateConfigForSchema(p.Descriptor().ConfigurationSchema, config); err != nil {
		return err
	}
	if len(config.Values) != 1 || len(config.Secrets) != 0 {
		return storageproto.ErrProtocol
	}
	raw := config.Values["root"]
	var root string
	if err := json.Unmarshal(raw, &root); err != nil {
		return storageproto.ErrProtocol
	}
	if !filepath.IsAbs(root) || len(root) == 0 || len(root) > 4096 || strings.ContainsRune(root, '\x00') {
		return fmt.Errorf("local storage root is invalid")
	}
	clean := filepath.Clean(root)
	if clean != root || clean == string(filepath.Separator) {
		return fmt.Errorf("local storage root is invalid")
	}
	p.mu.Lock()
	if p.root != clean {
		p.clearDigestCacheLocked()
		p.rootEpoch++
	}
	p.root = clean
	p.mu.Unlock()
	return nil
}

// Probe safely opens the configured archive root, creating its final
// directory component when it does not yet exist.
func (p *Provider) Probe(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	root, err := p.configuredRoot()
	if err != nil {
		return err
	}
	fd, err := openRootPath(root, true)
	if err != nil {
		return fmt.Errorf("open local storage root: %w", err)
	}
	return closeFD(fd)
}

func (p *Provider) Put(ctx context.Context, key string, source io.Reader, size int64) (storageproto.ObjectInfo, error) {
	if err := contextError(ctx); err != nil {
		return storageproto.ObjectInfo{}, err
	}
	if err := storageproto.ValidateKey(key); err != nil {
		return storageproto.ObjectInfo{}, err
	}
	if source == nil || size < 0 || size > storageproto.MaxObjectBytes {
		return storageproto.ObjectInfo{}, fmt.Errorf("local storage object size is invalid")
	}
	components := objectComponents(key)
	root, rootEpoch, err := p.openConfiguredRootVersioned()
	if err != nil {
		return storageproto.ObjectInfo{}, err
	}
	defer closeFD(root)
	parent, leaf, err := openParent(root, components, true)
	if err != nil {
		return storageproto.ObjectInfo{}, err
	}
	defer closeFD(parent)
	if err = validateReplaceTarget(parent, leaf); err != nil {
		return storageproto.ObjectInfo{}, err
	}

	temporaryName, err := makeTemporaryName(temporaryPrefix)
	if err != nil {
		return storageproto.ObjectInfo{}, err
	}
	temporaryFD, err := openFileAt(parent, temporaryName, openWriteCreateExclusive, 0600)
	if err != nil {
		return storageproto.ObjectInfo{}, fmt.Errorf("create local storage temporary object: %w", err)
	}
	temporary := os.NewFile(uintptr(temporaryFD), "local-storage-temporary")
	published := false
	defer func() {
		if !published {
			_ = unlinkAt(parent, temporaryName)
		}
	}()

	hash := sha256.New()
	count, copyErr := io.Copy(io.MultiWriter(temporary, hash), &contextReader{
		ctx:    ctx,
		reader: io.LimitReader(source, size+1),
	})
	if copyErr != nil {
		_ = temporary.Close()
		return storageproto.ObjectInfo{}, copyErr
	}
	if count != size {
		_ = temporary.Close()
		return storageproto.ObjectInfo{}, fmt.Errorf("local storage object size does not match content length")
	}
	if err = ctx.Err(); err != nil {
		_ = temporary.Close()
		return storageproto.ObjectInfo{}, err
	}
	if err = temporary.Sync(); err != nil {
		_ = temporary.Close()
		return storageproto.ObjectInfo{}, fmt.Errorf("sync local storage temporary object: %w", err)
	}
	if err = ctx.Err(); err != nil {
		_ = temporary.Close()
		return storageproto.ObjectInfo{}, err
	}
	if err = renameAt(parent, temporaryName, parent, leaf); err != nil {
		_ = temporary.Close()
		return storageproto.ObjectInfo{}, fmt.Errorf("publish local storage object: %w", err)
	}
	published = true
	if err = fsyncDirectoryFD(parent); err != nil {
		_ = temporary.Close()
		return storageproto.ObjectInfo{}, fmt.Errorf("sync local storage object directory: %w", err)
	}
	if err = ctx.Err(); err != nil {
		_ = temporary.Close()
		return storageproto.ObjectInfo{}, err
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	// The still-open file descriptor identifies the exact inode just published.
	// Only cache the streaming digest after publication and directory durability;
	// a later replacement will have a different descriptor signature.
	var publishedVersion objectVersion
	cacheable := false
	if info, statErr := temporary.Stat(); statErr == nil && info.Mode().IsRegular() && info.Size() == count && fileLinkCount(info) == 1 {
		publishedVersion, cacheable = versionOf(info)
	}
	if err = temporary.Close(); err != nil {
		return storageproto.ObjectInfo{}, fmt.Errorf("close published local storage object: %w", err)
	}
	if cacheable {
		p.storeDigest(key, rootEpoch, publishedVersion, digest)
	}
	return storageproto.ObjectInfo{Size: count, SHA256: digest}, nil
}

func (p *Provider) Open(ctx context.Context, key string) (io.ReadCloser, storageproto.ObjectInfo, error) {
	file, info, err := p.openAndDescribe(ctx, key)
	if err != nil {
		return nil, storageproto.ObjectInfo{}, err
	}
	return &contextReadCloser{ctx: ctx, file: file}, info, nil
}

func (p *Provider) OpenRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storageproto.ObjectInfo, error) {
	if err := contextError(ctx); err != nil {
		return nil, storageproto.ObjectInfo{}, err
	}
	file, info, err := p.openAndDescribe(ctx, key)
	if err != nil {
		return nil, storageproto.ObjectInfo{}, err
	}
	if offset < 0 || length <= 0 || offset > info.Size || length > info.Size-offset {
		_ = file.Close()
		return nil, storageproto.ObjectInfo{}, storageproto.ErrInvalidRange
	}
	if _, err = file.Seek(offset, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, storageproto.ObjectInfo{}, err
	}
	limited := io.LimitReader(&contextReader{ctx: ctx, reader: file}, length)
	return &limitedReadCloser{Reader: limited, Closer: file}, info, nil
}

func (p *Provider) Stat(ctx context.Context, key string) (storageproto.ObjectInfo, error) {
	file, info, err := p.openAndDescribe(ctx, key)
	if file != nil {
		_ = file.Close()
	}
	return info, err
}

func (p *Provider) List(ctx context.Context, prefix, cursor string, limit int) (storageproto.ListPage, error) {
	return p.listWithLimits(ctx, prefix, cursor, limit, listLimits{
		maxEntries: maxListVisitedEntries,
		maxDepth:   maxListTraversalDepth,
	})
}

func (p *Provider) listWithLimits(ctx context.Context, prefix, cursor string, limit int, limits listLimits) (storageproto.ListPage, error) {
	if err := contextError(ctx); err != nil {
		return storageproto.ListPage{}, err
	}
	if err := storageproto.ValidatePrefix(prefix); err != nil {
		return storageproto.ListPage{}, err
	}
	if cursor != "" && storageproto.ValidateKey(cursor) != nil || len(cursor) > 1024 {
		return storageproto.ListPage{}, storageproto.ErrInvalidKey
	}
	if limit < 1 || limit > storageproto.MaxListLimit {
		return storageproto.ListPage{}, fmt.Errorf("local storage list limit is invalid")
	}
	if limits.maxEntries < 1 || limits.maxDepth < 0 {
		return storageproto.ListPage{}, errListLimit
	}
	root, err := p.openConfiguredRoot()
	if err != nil {
		return storageproto.ListPage{}, err
	}
	defer closeFD(root)

	selected := make([]storageproto.ObjectEntry, 0, limit+1)
	budget := listBudget{limits: limits}
	if prefixCanMatchRecordings(prefix) {
		components := listPrefixDirectoryComponents(prefix, true)
		startFD, relative, depth, ok, startErr := openListSubtree(ctx, root, components, 0, true, &budget)
		if startErr != nil {
			return storageproto.ListPage{}, startErr
		}
		if ok {
			err = p.walkTree(ctx, startFD, relative, true, prefix, cursor, limit+1, depth, &budget, &selected)
			_ = closeFD(startFD)
			if err != nil {
				return storageproto.ListPage{}, err
			}
		}
	}
	if prefixCanMatchPrivate(prefix) {
		privateFD, privateErr := openDirAt(root, privateDirectory)
		if privateErr == nil {
			otherFD, otherErr := openDirAt(privateFD, privateObjects)
			_ = closeFD(privateFD)
			if otherErr == nil {
				if err = budget.visit(2); err != nil {
					_ = closeFD(otherFD)
					return storageproto.ListPage{}, err
				}
				components := listPrefixDirectoryComponents(prefix, false)
				startFD, relative, depth, ok, startErr := openListSubtree(ctx, otherFD, components, 2, false, &budget)
				_ = closeFD(otherFD)
				if startErr != nil {
					return storageproto.ListPage{}, startErr
				}
				if ok {
					err = p.walkTree(ctx, startFD, relative, false, prefix, cursor, limit+1, depth, &budget, &selected)
					_ = closeFD(startFD)
					if err != nil {
						return storageproto.ListPage{}, err
					}
				}
			} else if !isMissing(otherErr) {
				return storageproto.ListPage{}, fmt.Errorf("open local storage private object directory: %w", otherErr)
			}
		} else if !isMissing(privateErr) {
			return storageproto.ListPage{}, fmt.Errorf("open local storage private directory: %w", privateErr)
		}
	}

	sortEntries(selected)
	page := storageproto.ListPage{Items: make([]storageproto.ObjectEntry, 0, min(limit, len(selected)))}
	more := len(selected) > limit
	if more {
		selected = selected[:limit]
	}
	page.Items = append(page.Items, selected...)
	if more && len(page.Items) != 0 {
		page.NextCursor = page.Items[len(page.Items)-1].Key
	}
	return page, nil
}

func (p *Provider) Delete(ctx context.Context, key string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := storageproto.ValidateKey(key); err != nil {
		return err
	}
	defer p.invalidateDigest(key)
	root, err := p.openConfiguredRoot()
	if err != nil {
		if isMissing(err) {
			return nil
		}
		return err
	}
	defer closeFD(root)
	parent, leaf, err := openParent(root, objectComponents(key), false)
	if err != nil {
		if isMissing(err) {
			return nil
		}
		return err
	}
	defer closeFD(parent)
	file, _, err := openRegularAt(parent, leaf)
	if isMissing(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = unlinkAt(parent, leaf); isMissing(err) {
		return nil
	} else if err != nil {
		return err
	}
	return fsyncDirectoryFD(parent)
}

func (p *Provider) openAndDescribe(ctx context.Context, key string) (*os.File, storageproto.ObjectInfo, error) {
	if err := contextError(ctx); err != nil {
		return nil, storageproto.ObjectInfo{}, err
	}
	if err := storageproto.ValidateKey(key); err != nil {
		return nil, storageproto.ObjectInfo{}, err
	}
	root, rootEpoch, err := p.openConfiguredRootVersioned()
	if err != nil {
		return nil, storageproto.ObjectInfo{}, err
	}
	defer closeFD(root)
	parent, leaf, err := openParent(root, objectComponents(key), false)
	if err != nil {
		if isMissing(err) {
			return nil, storageproto.ObjectInfo{}, storageproto.ErrNotFound
		}
		return nil, storageproto.ObjectInfo{}, err
	}
	defer closeFD(parent)
	file, before, err := openRegularAt(parent, leaf)
	if err != nil {
		if isMissing(err) {
			return nil, storageproto.ObjectInfo{}, storageproto.ErrNotFound
		}
		return nil, storageproto.ObjectInfo{}, err
	}
	if before.Size() < 0 || before.Size() > storageproto.MaxObjectBytes {
		_ = file.Close()
		return nil, storageproto.ObjectInfo{}, fmt.Errorf("local storage object size is invalid")
	}
	beforeVersion, versioned := versionOf(before)
	if versioned {
		if digest, hit := p.cachedDigest(key, rootEpoch, beforeVersion); hit {
			after, statErr := file.Stat()
			if statErr != nil || !after.Mode().IsRegular() || fileLinkCount(after) != 1 || after.Size() != before.Size() || !os.SameFile(before, after) {
				_ = file.Close()
				return nil, storageproto.ObjectInfo{}, fmt.Errorf("local storage object changed while reading")
			}
			afterVersion, afterVersioned := versionOf(after)
			if !afterVersioned || afterVersion != beforeVersion {
				_ = file.Close()
				return nil, storageproto.ObjectInfo{}, fmt.Errorf("local storage object changed while reading")
			}
			if _, err = file.Seek(0, io.SeekStart); err != nil {
				_ = file.Close()
				return nil, storageproto.ObjectInfo{}, err
			}
			return file, storageproto.ObjectInfo{Size: before.Size(), SHA256: digest}, nil
		}
	}
	hash := sha256.New()
	count, err := io.Copy(hash, &contextReader{ctx: ctx, reader: file})
	if err != nil {
		_ = file.Close()
		return nil, storageproto.ObjectInfo{}, err
	}
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || fileLinkCount(after) != 1 || count != before.Size() || after.Size() != before.Size() || !os.SameFile(before, after) {
		_ = file.Close()
		return nil, storageproto.ObjectInfo{}, fmt.Errorf("local storage object changed while reading")
	}
	afterVersion, afterVersioned := versionOf(after)
	if versioned && (!afterVersioned || afterVersion != beforeVersion) {
		_ = file.Close()
		return nil, storageproto.ObjectInfo{}, fmt.Errorf("local storage object changed while reading")
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, storageproto.ObjectInfo{}, err
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if versioned {
		p.storeDigest(key, rootEpoch, beforeVersion, digest)
	}
	return file, storageproto.ObjectInfo{Size: count, SHA256: digest}, nil
}

func (p *Provider) cachedDigest(key string, rootEpoch uint64, version objectVersion) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if rootEpoch != p.rootEpoch {
		return "", false
	}
	element := p.digests[key]
	if element == nil {
		return "", false
	}
	entry := element.Value.(digestCacheEntry)
	if entry.version != version {
		return "", false
	}
	p.digestLRU.MoveToFront(element)
	p.digestCacheHits++
	return entry.digest, true
}

func (p *Provider) storeDigest(key string, rootEpoch uint64, version objectVersion, digest string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if rootEpoch != p.rootEpoch {
		return
	}
	if p.digests == nil {
		p.digests = make(map[string]*list.Element)
	}
	if element := p.digests[key]; element != nil {
		element.Value = digestCacheEntry{key: key, version: version, digest: digest}
		p.digestLRU.MoveToFront(element)
		return
	}
	element := p.digestLRU.PushFront(digestCacheEntry{key: key, version: version, digest: digest})
	p.digests[key] = element
	if p.digestLRU.Len() > maxDigestCacheEntries {
		oldest := p.digestLRU.Back()
		entry := oldest.Value.(digestCacheEntry)
		delete(p.digests, entry.key)
		p.digestLRU.Remove(oldest)
	}
}

func (p *Provider) invalidateDigest(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if element := p.digests[key]; element != nil {
		delete(p.digests, key)
		p.digestLRU.Remove(element)
	}
}

func (p *Provider) clearDigestCacheLocked() {
	p.digests = nil
	p.digestLRU.Init()
}

// digestCacheHitCount is intentionally package-private and used only by tests
// to prove cache behavior without timing-dependent assertions.
func (p *Provider) digestCacheHitCount() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.digestCacheHits
}

func prefixCanMatchRecordings(prefix string) bool {
	return prefix == "" || strings.HasPrefix("recordings/", prefix) || strings.HasPrefix(prefix, "recordings/")
}

func prefixCanMatchPrivate(prefix string) bool {
	return prefix == "" || !strings.HasPrefix(prefix, "recordings/")
}

// listPrefixDirectoryComponents returns only path components known to be
// directories from prefix. A partial final component must remain in walkTree
// because List uses lexical key-prefix matching, not component matching.
func listPrefixDirectoryComponents(prefix string, recordings bool) []string {
	if prefix == "" {
		return nil
	}
	if recordings {
		if !strings.HasPrefix(prefix, "recordings/") {
			return nil
		}
		prefix = strings.TrimPrefix(prefix, "recordings/")
	}
	trailingSlash := strings.HasSuffix(prefix, "/")
	if trailingSlash {
		prefix = strings.TrimSuffix(prefix, "/")
	}
	if prefix == "" {
		return nil
	}
	components := strings.Split(prefix, "/")
	if !trailingSlash {
		components = components[:len(components)-1]
	}
	return components
}

// openListSubtree opens the deepest directory guaranteed by prefix. It uses
// descriptor-relative no-follow opens and treats missing, non-directory, or
// symlinked prefix components as an empty match, as a full walk would.
func openListSubtree(ctx context.Context, root int, components []string, depth int, recordings bool, budget *listBudget) (int, string, int, bool, error) {
	fd, err := duplicateFD(root)
	if err != nil {
		return -1, "", depth, false, err
	}
	physicalParts := make([]string, 0, len(components))
	for _, component := range components {
		if err = ctx.Err(); err != nil {
			_ = closeFD(fd)
			return -1, "", depth, false, err
		}
		depth++
		if err = budget.visit(depth); err != nil {
			_ = closeFD(fd)
			return -1, "", depth, false, err
		}
		physicalParts = append(physicalParts, component)
		physical := strings.Join(physicalParts, "/")
		logical := physical
		if recordings {
			logical = "recordings/" + physical
		}
		budget.observe(logical)
		next, openErr := openDirAt(fd, component)
		if openErr != nil {
			_ = closeFD(fd)
			if isMissing(openErr) || isNotDirectory(openErr) || isSymlink(openErr) || isUnsafeEntry(openErr) {
				return -1, "", depth, false, nil
			}
			return -1, "", depth, false, fmt.Errorf("open local storage listing prefix: %w", openErr)
		}
		_ = closeFD(fd)
		fd = next
	}
	return fd, strings.Join(physicalParts, "/"), depth, true, nil
}

func (p *Provider) walkTree(ctx context.Context, directory int, relative string, recordings bool, prefix, cursor string, retain, depth int, budget *listBudget, selected *[]storageproto.ObjectEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if depth > budget.limits.maxDepth {
		return errListLimit
	}
	copyFD, err := duplicateFD(directory)
	if err != nil {
		return err
	}
	dir := os.NewFile(uintptr(copyFD), "local-storage-list-directory")
	if dir == nil {
		return fmt.Errorf("open local storage listing directory")
	}
	defer dir.Close()
	for {
		entries, err := dir.ReadDir(readDirBatchSize)
		for _, entry := range entries {
			if contextErr := ctx.Err(); contextErr != nil {
				return contextErr
			}
			entryDepth := depth + 1
			if err := budget.visit(entryDepth); err != nil {
				return err
			}
			name := entry.Name()
			if name == "" || name == "." || name == ".." || strings.Contains(name, "/") || strings.HasPrefix(name, temporaryPrefix) {
				continue
			}
			if relative == "" && recordings && name == privateDirectory {
				continue
			}
			physical := name
			if relative != "" {
				physical = relative + "/" + name
			}
			logical := physical
			if recordings {
				logical = "recordings/" + physical
			}
			if storageproto.ValidateKey(logical) != nil || len(physical) > 1024 || !recordings && strings.HasPrefix(logical, "recordings/") {
				continue
			}
			budget.observe(logical)

			logicalDirectory := physical + "/"
			if recordings {
				logicalDirectory = "recordings/" + logicalDirectory
			}
			if !directoryMayMatchPrefix(logicalDirectory, prefix) {
				continue
			}
			childFD, openErr := openDirAt(int(dir.Fd()), name)
			if openErr == nil {
				// A directory can contain matching keys only when its logical path
				// and the requested prefix overlap. Prune unrelated subtrees before
				// opening their contents. Keep lexical partial-component matches:
				// prefix "recordings/ab" must still include both "ab/..." and
				// "abc/..." keys.
				childErr := p.walkTree(ctx, childFD, physical, recordings, prefix, cursor, retain, entryDepth, budget, selected)
				_ = closeFD(childFD)
				if childErr != nil {
					return childErr
				}
				continue
			}
			if isMissing(openErr) {
				continue
			}
			if !isNotDirectory(openErr) && !isSymlink(openErr) {
				return fmt.Errorf("open local storage listing entry: %w", openErr)
			}
			file, info, fileErr := openRegularAt(int(dir.Fd()), name)
			if fileErr != nil {
				if isMissing(fileErr) || isSymlink(fileErr) || errors.Is(fileErr, errUnsafeEntry) || isUnsafeEntry(fileErr) {
					continue
				}
				return fmt.Errorf("inspect local storage listing entry: %w", fileErr)
			}
			closeErr := file.Close()
			if closeErr != nil {
				return closeErr
			}
			if info.Size() < 0 || info.Size() > storageproto.MaxObjectBytes || !strings.HasPrefix(logical, prefix) || logical <= cursor {
				continue
			}
			insertBounded(selected, storageproto.ObjectEntry{Key: logical, Size: info.Size()}, retain)
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read local storage listing directory: %w", err)
		}
	}
}

// directoryMayMatchPrefix reports whether a logical directory can contain a
// key with prefix. Directories include a trailing slash so component-boundary
// prefixes prune sibling names without changing lexical List semantics.
func directoryMayMatchPrefix(directory, prefix string) bool {
	if prefix == "" || directory == "" {
		return true
	}
	return strings.HasPrefix(directory, prefix) || strings.HasPrefix(prefix, directory)
}

func (p *Provider) configuredRoot() (string, error) {
	p.mu.RLock()
	root := p.root
	p.mu.RUnlock()
	if root == "" {
		return "", fmt.Errorf("local storage provider is not configured")
	}
	return root, nil
}

func (p *Provider) configuredRootEpoch() (string, uint64, error) {
	p.mu.RLock()
	root, epoch := p.root, p.rootEpoch
	p.mu.RUnlock()
	if root == "" {
		return "", 0, fmt.Errorf("local storage provider is not configured")
	}
	return root, epoch, nil
}

func (p *Provider) openConfiguredRoot() (int, error) {
	root, err := p.configuredRoot()
	if err != nil {
		return -1, err
	}
	fd, err := openRootPath(root, false)
	if err != nil {
		return -1, fmt.Errorf("open local storage root: %w", err)
	}
	return fd, nil
}

func (p *Provider) openConfiguredRootVersioned() (int, uint64, error) {
	root, epoch, err := p.configuredRootEpoch()
	if err != nil {
		return -1, 0, err
	}
	fd, err := openRootPath(root, false)
	if err != nil {
		return -1, 0, fmt.Errorf("open local storage root: %w", err)
	}
	return fd, epoch, nil
}

func objectComponents(key string) []string {
	if strings.HasPrefix(key, "recordings/") {
		return strings.Split(strings.TrimPrefix(key, "recordings/"), "/")
	}
	components := strings.Split(key, "/")
	return append([]string{privateDirectory, privateObjects}, components...)
}

func openParent(root int, components []string, create bool) (int, string, error) {
	if len(components) == 0 {
		return -1, "", storageproto.ErrInvalidKey
	}
	parent, err := duplicateFD(root)
	if err != nil {
		return -1, "", err
	}
	for _, component := range components[:len(components)-1] {
		next, openErr := openDirAt(parent, component)
		if openErr != nil && create && isMissing(openErr) {
			if mkdirErr := mkdirAt(parent, component, 0700); mkdirErr != nil && !isAlreadyExists(mkdirErr) {
				_ = closeFD(parent)
				return -1, "", mkdirErr
			}
			if syncErr := fsyncDirectoryFD(parent); syncErr != nil {
				_ = closeFD(parent)
				return -1, "", syncErr
			}
			next, openErr = openDirAt(parent, component)
		}
		if openErr != nil {
			_ = closeFD(parent)
			return -1, "", normalizeMissing(openErr)
		}
		_ = closeFD(parent)
		parent = next
	}
	return parent, components[len(components)-1], nil
}

func validateReplaceTarget(parent int, leaf string) error {
	file, _, err := openRegularAt(parent, leaf)
	if err == nil {
		return file.Close()
	}
	if isMissing(err) {
		return nil
	}
	return err
}

func openRegularAt(parent int, name string) (*os.File, os.FileInfo, error) {
	fd, err := openFileAt(parent, name, openReadNoFollow, 0)
	if err != nil {
		return nil, nil, err
	}
	file := os.NewFile(uintptr(fd), "local-storage-object")
	if file == nil {
		_ = closeFD(fd)
		return nil, nil, fmt.Errorf("open local storage object")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() || fileLinkCount(info) != 1 {
		_ = file.Close()
		return nil, nil, errUnsafeEntry
	}
	return file, info, nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("storage operation context is required")
	}
	return ctx.Err()
}

func normalizeMissing(err error) error {
	if isMissing(err) {
		return storageproto.ErrNotFound
	}
	return err
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}

type contextReadCloser struct {
	ctx  context.Context
	file *os.File
}

func (r *contextReadCloser) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.file.Read(buffer)
}

func (r *contextReadCloser) Close() error { return r.file.Close() }

type limitedReadCloser struct {
	io.Reader
	io.Closer
}

func insertBounded(entries *[]storageproto.ObjectEntry, candidate storageproto.ObjectEntry, retain int) {
	items := *entries
	if len(items) < retain {
		*entries = append(items, candidate)
		return
	}
	largest := 0
	for index := 1; index < len(items); index++ {
		if items[index].Key > items[largest].Key {
			largest = index
		}
	}
	if candidate.Key < items[largest].Key {
		items[largest] = candidate
	}
}

func sortEntries(entries []storageproto.ObjectEntry) {
	sort.Slice(entries, func(left, right int) bool { return entries[left].Key < entries[right].Key })
}

type listLimits struct {
	maxEntries int
	maxDepth   int
	onVisit    func(string)
}

type listBudget struct {
	limits  listLimits
	visited int
}

func (budget *listBudget) visit(depth int) error {
	if depth > budget.limits.maxDepth || budget.visited >= budget.limits.maxEntries {
		return errListLimit
	}
	budget.visited++
	return nil
}

func (budget *listBudget) observe(logical string) {
	if budget.limits.onVisit != nil {
		budget.limits.onVisit(logical)
	}
}
