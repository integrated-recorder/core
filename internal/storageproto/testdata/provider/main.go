package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/integrated-recorder/core/internal/storageproto"
)

type provider struct {
	root          string
	ownedRoot     bool
	crashKey      string
	crashPhase    string
	crashMarker   string
	crashArmed    bool
	launchMarker  string
	attemptMarker string
}

func main() {
	socket := flag.String("socket", "", "")
	token := flag.String("token-file", "", "")
	flag.Parse()
	if *socket == "" || *token == "" || flag.NArg() != 0 {
		os.Exit(2)
	}
	p := &provider{}
	ctx, cancel := signalContext()
	defer cancel()
	err := storageproto.Serve(ctx, p, storageproto.ServeOptions{
		SocketPath:         *socket,
		TokenFile:          *token,
		MaxConcurrentReads: storageproto.MaxConcurrentReadStreams,
	})
	p.cleanup()
	if err != nil {
		fmt.Fprintln(os.Stderr, "storage fixture stopped:", err)
		os.Exit(1)
	}
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func (p *provider) Descriptor() storageproto.Descriptor {
	return storageproto.Descriptor{ProtocolVersion: 1, ID: "fixture-storage", Name: "Fixture Storage", Version: "1.0.0", ConfigurationSchema: storageproto.Schema{Fields: []storageproto.Field{
		{Key: "root", Control: "text", Label: "Object directory"},
		// These optional controls are limited to the repository's external test
		// executable. They exercise child-crash recovery without changing Core's
		// production Storage Provider Protocol implementation.
		{Key: "crash_key", Control: "text", Label: "Test crash object key", Default: json.RawMessage(`""`)},
		{Key: "crash_phase", Control: "text", Label: "Test crash phase", Default: json.RawMessage(`""`)},
		{Key: "crash_marker", Control: "text", Label: "Test crash marker", Default: json.RawMessage(`""`)},
		{Key: "launch_marker", Control: "text", Label: "Test launch marker", Default: json.RawMessage(`""`)},
		{Key: "attempt_marker", Control: "text", Label: "Test put attempt marker", Default: json.RawMessage(`""`)},
	}}, Capabilities: []string{storageproto.CapabilityRead, storageproto.CapabilityWrite, storageproto.CapabilityStat, storageproto.CapabilityList, storageproto.CapabilityDelete, storageproto.CapabilityRangeRead, storageproto.CapabilityAtomicReplace}}
}

func (p *provider) Configure(_ context.Context, config storageproto.Config) error {
	if raw := config.Values["root"]; len(raw) > 0 {
		var root string
		if err := json.Unmarshal(raw, &root); err != nil || !filepath.IsAbs(root) {
			return errors.New("invalid configuration")
		}
		p.root = root
	} else if p.root == "" {
		root, err := os.MkdirTemp("", "ir-storage-provider-")
		if err != nil {
			return err
		}
		p.root, p.ownedRoot = root, true
	}
	if err := os.MkdirAll(p.root, 0700); err != nil {
		return err
	}
	var err error
	if p.crashKey, err = configuredText(config, "crash_key"); err != nil {
		return err
	}
	if p.crashPhase, err = configuredText(config, "crash_phase"); err != nil {
		return err
	}
	if p.crashMarker, err = configuredText(config, "crash_marker"); err != nil {
		return err
	}
	if p.launchMarker, err = configuredText(config, "launch_marker"); err != nil {
		return err
	}
	if p.attemptMarker, err = configuredText(config, "attempt_marker"); err != nil {
		return err
	}
	for _, path := range []string{p.crashMarker, p.launchMarker, p.attemptMarker} {
		if path != "" && (!filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 4096) {
			return errors.New("invalid test marker path")
		}
	}
	if p.crashPhase != "" && p.crashPhase != "before_first_byte" && p.crashPhase != "mid_stream" && p.crashPhase != "after_atomic_publish_before_response" {
		return errors.New("invalid test crash phase")
	}
	if p.crashPhase != "" && (p.crashKey == "" || p.crashMarker == "") {
		return errors.New("incomplete test crash configuration")
	}
	p.crashArmed = p.crashPhase != ""
	if p.crashArmed {
		if _, err := os.Lstat(p.crashMarker); err == nil {
			p.crashArmed = false
		} else if !errors.Is(err, os.ErrNotExist) {
			return errors.New("test crash marker is unavailable")
		}
	}
	if p.launchMarker != "" {
		if err := appendLaunchMarker(p.launchMarker, os.Getpid()); err != nil {
			return err
		}
	}
	return nil
}
func (p *provider) Probe(context.Context) error {
	if p.root == "" {
		return errors.New("not configured")
	}
	return os.MkdirAll(p.root, 0700)
}

func (p *provider) Put(ctx context.Context, key string, source io.Reader, size int64) (storageproto.ObjectInfo, error) {
	if err := storageproto.ValidateKey(key); err != nil {
		return storageproto.ObjectInfo{}, err
	}
	if size < 0 || size > storageproto.MaxObjectBytes {
		return storageproto.ObjectInfo{}, errors.New("invalid size")
	}
	if p.crashKey == key && p.attemptMarker != "" {
		if err := appendAttemptMarker(p.attemptMarker, key, os.Getpid()); err != nil {
			return storageproto.ObjectInfo{}, err
		}
	}
	path, err := p.objectPath(key)
	if err != nil {
		return storageproto.ObjectInfo{}, err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return storageproto.ObjectInfo{}, err
	}
	if p.crashArmed && p.crashKey == key && p.crashPhase == "before_first_byte" {
		if crash, err := p.signalOneShotCrash(); err != nil {
			return storageproto.ObjectInfo{}, err
		} else if crash {
			os.Exit(71)
		}
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".ir-object-*")
	if err != nil {
		return storageproto.ObjectInfo{}, err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	hash := sha256.New()
	if p.crashArmed && p.crashKey == key && p.crashPhase == "mid_stream" {
		partial := size
		if partial > 4096 {
			partial = 4096
		}
		if partial > 0 {
			if _, err := io.CopyN(tmp, &contextReader{ctx: ctx, reader: source}, partial); err != nil {
				_ = tmp.Close()
				return storageproto.ObjectInfo{}, err
			}
		}
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			return storageproto.ObjectInfo{}, err
		}
		if crash, err := p.signalOneShotCrash(); err != nil {
			_ = tmp.Close()
			return storageproto.ObjectInfo{}, err
		} else if crash {
			os.Exit(72)
		}
	}
	count, copyErr := io.Copy(io.MultiWriter(tmp, hash), &contextReader{ctx: ctx, reader: io.LimitReader(source, size+1)})
	if copyErr != nil {
		_ = tmp.Close()
		return storageproto.ObjectInfo{}, copyErr
	}
	if count != size {
		_ = tmp.Close()
		return storageproto.ObjectInfo{}, fmt.Errorf("size mismatch")
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return storageproto.ObjectInfo{}, err
	}
	if err = tmp.Close(); err != nil {
		return storageproto.ObjectInfo{}, err
	}
	if err = ctx.Err(); err != nil {
		return storageproto.ObjectInfo{}, err
	}
	if err = os.Rename(tmpPath, path); err != nil {
		return storageproto.ObjectInfo{}, err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err == nil {
		err = dir.Sync()
		_ = dir.Close()
	}
	if err != nil {
		return storageproto.ObjectInfo{}, err
	}
	if p.crashArmed && p.crashKey == key && p.crashPhase == "after_atomic_publish_before_response" {
		if crash, err := p.signalOneShotCrash(); err != nil {
			return storageproto.ObjectInfo{}, err
		} else if crash {
			os.Exit(73)
		}
	}
	return storageproto.ObjectInfo{Size: count, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func configuredText(config storageproto.Config, key string) (string, error) {
	raw := config.Values[key]
	if len(raw) == 0 {
		return "", nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || len(value) > 4096 {
		return "", errors.New("invalid test configuration")
	}
	return value, nil
}

func (p *provider) signalOneShotCrash() (bool, error) {
	if p.crashMarker == "" {
		return false, nil
	}
	marker, err := os.OpenFile(p.crashMarker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		return false, nil
	}
	if err != nil {
		return false, errors.New("test crash marker is unavailable")
	}
	if _, err := fmt.Fprintf(marker, "%d\n", os.Getpid()); err != nil {
		_ = marker.Close()
		return false, err
	}
	if err := marker.Sync(); err != nil {
		_ = marker.Close()
		return false, err
	}
	if err := marker.Close(); err != nil {
		return false, err
	}
	return true, nil
}

func appendLaunchMarker(path string, pid int) error {
	marker, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return errors.New("test launch marker is unavailable")
	}
	if _, err := fmt.Fprintf(marker, "%d\n", pid); err != nil {
		_ = marker.Close()
		return err
	}
	if err := marker.Sync(); err != nil {
		_ = marker.Close()
		return err
	}
	return marker.Close()
}

func appendAttemptMarker(path, key string, pid int) error {
	marker, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return errors.New("test attempt marker is unavailable")
	}
	if _, err := fmt.Fprintf(marker, "%d %s\n", pid, key); err != nil {
		_ = marker.Close()
		return err
	}
	if err := marker.Sync(); err != nil {
		_ = marker.Close()
		return err
	}
	return marker.Close()
}

func (p *provider) Open(ctx context.Context, key string) (io.ReadCloser, storageproto.ObjectInfo, error) {
	path, err := p.objectPath(key)
	if err != nil {
		return nil, storageproto.ObjectInfo{}, err
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, storageproto.ObjectInfo{}, storageproto.ErrNotFound
	}
	if err != nil {
		return nil, storageproto.ObjectInfo{}, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, storageproto.ObjectInfo{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > storageproto.MaxObjectBytes {
		_ = f.Close()
		return nil, storageproto.ObjectInfo{}, errors.New("invalid object")
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, &contextReader{ctx: ctx, reader: f}); err != nil {
		_ = f.Close()
		return nil, storageproto.ObjectInfo{}, err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, storageproto.ObjectInfo{}, err
	}
	return f, storageproto.ObjectInfo{Size: info.Size(), SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func (p *provider) OpenRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, storageproto.ObjectInfo, error) {
	full, info, err := p.Open(ctx, key)
	if err != nil {
		return nil, storageproto.ObjectInfo{}, err
	}
	if offset < 0 || length <= 0 || offset+length > info.Size {
		_ = full.Close()
		return nil, storageproto.ObjectInfo{}, storageproto.ErrInvalidRange
	}
	seeker, ok := full.(io.Seeker)
	if !ok {
		_ = full.Close()
		return nil, storageproto.ObjectInfo{}, errors.New("range unsupported")
	}
	if _, err = seeker.Seek(offset, io.SeekStart); err != nil {
		_ = full.Close()
		return nil, storageproto.ObjectInfo{}, err
	}
	return &limitedReadCloser{Reader: io.LimitReader(full, length), Closer: full}, info, nil
}

func (p *provider) Stat(ctx context.Context, key string) (storageproto.ObjectInfo, error) {
	r, info, err := p.Open(ctx, key)
	if r != nil {
		_ = r.Close()
	}
	return info, err
}

func (p *provider) List(ctx context.Context, prefix, cursor string, limit int) (storageproto.ListPage, error) {
	var keys []string
	err := filepath.WalkDir(p.root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if strings.HasPrefix(entry.Name(), ".ir-object-") {
			return nil
		}
		rel, err := filepath.Rel(p.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) && key > cursor {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return storageproto.ListPage{}, err
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	page := storageproto.ListPage{Items: make([]storageproto.ObjectEntry, 0, len(keys))}
	for _, key := range keys {
		info, err := p.Stat(ctx, key)
		if err != nil {
			return storageproto.ListPage{}, err
		}
		page.Items = append(page.Items, storageproto.ObjectEntry{Key: key, Size: info.Size, SHA256: info.SHA256})
	}
	if len(keys) == limit {
		more, err := p.hasMore(prefix, keys[len(keys)-1])
		if err != nil {
			return storageproto.ListPage{}, err
		}
		if more {
			page.NextCursor = keys[len(keys)-1]
		}
	}
	return page, nil
}

func (p *provider) hasMore(prefix, cursor string) (bool, error) {
	more := false
	err := filepath.WalkDir(p.root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".ir-object-") {
			return nil
		}
		rel, err := filepath.Rel(p.root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) && key > cursor {
			more = true
			return errors.New("stop walk")
		}
		return nil
	})
	if more {
		return true, nil
	}
	return false, err
}

func (p *provider) Delete(_ context.Context, key string) error {
	path, err := p.objectPath(key)
	if err != nil {
		return err
	}
	if err = os.Remove(path); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for dir := filepath.Dir(path); dir != p.root && strings.HasPrefix(dir, p.root+string(filepath.Separator)); dir = filepath.Dir(dir) {
		if os.Remove(dir) != nil {
			break
		}
	}
	return nil
}

func (p *provider) objectPath(key string) (string, error) {
	if err := storageproto.ValidateKey(key); err != nil {
		return "", err
	}
	if p.root == "" {
		return "", errors.New("not configured")
	}
	path := filepath.Join(p.root, filepath.FromSlash(key))
	rel, err := filepath.Rel(p.root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", storageproto.ErrInvalidKey
	}
	return path, nil
}

func (p *provider) cleanup() {
	if p.ownedRoot {
		_ = os.RemoveAll(p.root)
	}
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

type limitedReadCloser struct {
	io.Reader
	io.Closer
}
