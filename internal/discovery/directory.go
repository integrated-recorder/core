// Package discovery implements untrusted rendezvous directory storage. It
// transports signed NodeAdvertisements but never decides identity authority.
package discovery

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/discoveryprotocol"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
	"github.com/integrated-recorder/core/internal/signalingprotocol"
)

const (
	directoryStateFile = "directory-state-v1.json"
	directoryFileMode  = 0600
	directoryMode      = 0700
	maxDirectoryBytes  = 32 << 20
	maxDirectoryNodes  = 10000
	maxLookupResults   = 64
	maxPublishBody     = 40 << 10
	maxHistoryBody     = 6 << 20
	maxLookupBody      = 8 << 10
	maxRateEntries     = 10000
	requestsPerMinute  = 120
)

var (
	ErrDirectoryInvalid = errors.New("discovery directory state is invalid")
	ErrDirectoryReplay  = errors.New("discovery advertisement sequence is stale")
	ErrEquivocation     = errors.New("discovery node advertisement equivocation detected")
	ErrNodeFrozen       = errors.New("discovery node is frozen after equivocation")
	ErrDirectoryFull    = errors.New("discovery directory capacity is exhausted")
	ErrLookupLimit      = errors.New("discovery lookup result limit exceeded")
)

type SignedAdvertisement struct {
	Canonical    []byte
	Signature    []byte
	NodeID       string
	IdentityHint string
	Sequence     string
	ExpiresAt    time.Time
}

type signedValue struct {
	Canonical string `json:"canonical_advertisement"`
	Signature string `json:"signature"`
}

type signalLookupResponse struct {
	Messages []signalingprotocol.Message `json:"messages"`
}

type storedNode struct {
	NodeID       string        `json:"node_id"`
	IdentityHint string        `json:"identity_hint"`
	Sequence     string        `json:"sequence"`
	Canonical    string        `json:"canonical_advertisement"`
	Signature    string        `json:"signature"`
	ExpiresAt    time.Time     `json:"expires_at"`
	Frozen       bool          `json:"frozen"`
	ForkEvidence []signedValue `json:"fork_evidence,omitempty"`
}

type directoryState struct {
	SchemaVersion int          `json:"schema_version"`
	Nodes         []storedNode `json:"nodes"`
}

// Directory is a durable rendezvous index with one serialized writer per
// root. The index only accepts Node-Key-signed advertisements and preserves
// sequence high-water evidence after expiry. Multiple independent Directory
// deployments must use separate roots; this object is not a federation
// consensus service.
type Directory struct {
	mu          sync.Mutex
	root        string
	history     *globalidentity.HistoryRegistry
	signalMu    sync.Mutex
	signals     map[string][]relayedSignal
	signalBytes int
}

func OpenDirectory(root string) (*Directory, error) {
	if strings.TrimSpace(root) == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, fmt.Errorf("%w: root path", ErrDirectoryInvalid)
	}
	if err := os.MkdirAll(root, directoryMode); err != nil {
		return nil, errors.New("discovery directory storage is unavailable")
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("%w: root permissions", ErrDirectoryInvalid)
	}
	history, err := globalidentity.OpenHistoryRegistry(filepath.Join(root, "identity-history"))
	if err != nil {
		return nil, errors.New("discovery identity history storage is unavailable")
	}
	d := &Directory{root: root, history: history}
	d.mu.Lock()
	defer d.mu.Unlock()
	unlock, err := lockDirectory(root)
	if err != nil {
		return nil, fmt.Errorf("discovery directory lock: %w", err)
	}
	defer unlock()
	if _, err := d.readLocked(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return d, nil
}

// PublishIdentityHistory verifies and durably mirrors public signed identity
// history. Discovery is only an untrusted transport mirror: the registry
// validates signatures and fork evidence but does not become an authority.
func (d *Directory) PublishIdentityHistory(bundle globalidentity.HistoryBundle) error {
	if d == nil || d.history == nil {
		return ErrDirectoryInvalid
	}
	if _, err := globalidentity.SummarizeHistoryBundle(bundle); err != nil {
		return globalidentity.ErrHistoryInvalid
	}
	return d.history.Import(bundle)
}

// LookupIdentityHistory returns a defensive copy of a verified signed
// history mirror entry. Its import timestamp is intentionally not returned
// because source timestamps are not freshness evidence.
func (d *Directory) LookupIdentityHistory(identityID string) (globalidentity.HistoryBundle, bool, error) {
	if d == nil || d.history == nil || identityID == "" {
		return globalidentity.HistoryBundle{}, false, ErrDirectoryInvalid
	}
	return d.history.Get(identityID)
}

func (d *Directory) Publish(canonical, signature []byte, now time.Time) (bool, error) {
	if d == nil || now.IsZero() {
		return false, ErrDirectoryInvalid
	}
	verified, err := discoveryprotocol.Verify(canonical, signature, now)
	if err != nil {
		return false, discoveryprotocol.ErrInvalid
	}
	if !verified.NodeKeyValid {
		return false, discoveryprotocol.ErrInvalid
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	unlock, err := lockDirectory(d.root)
	if err != nil {
		return false, err
	}
	defer unlock()
	state, err := d.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		state = directoryState{SchemaVersion: 1, Nodes: []storedNode{}}
	} else if err != nil {
		return false, err
	}
	index := -1
	for i := range state.Nodes {
		if state.Nodes[i].NodeID == verified.Advertisement.NodeID {
			index = i
			break
		}
	}
	next := storedFromVerified(verified)
	if index < 0 {
		if len(state.Nodes) >= maxDirectoryNodes {
			return false, ErrDirectoryFull
		}
		state.Nodes = append(state.Nodes, next)
	} else {
		current := &state.Nodes[index]
		if current.Frozen {
			return false, ErrNodeFrozen
		}
		cmp := compareSequence(verified.Advertisement.Sequence, current.Sequence)
		if cmp < 0 {
			return false, ErrDirectoryReplay
		}
		if cmp == 0 {
			if current.Canonical == base64.RawURLEncoding.EncodeToString(canonical) && current.Signature == base64.RawURLEncoding.EncodeToString(signature) {
				return true, nil
			}
			current.Frozen = true
			current.ForkEvidence = []signedValue{
				{Canonical: current.Canonical, Signature: current.Signature},
				{Canonical: next.Canonical, Signature: next.Signature},
			}
			if err := d.writeLocked(state); err != nil {
				return false, err
			}
			return false, ErrEquivocation
		}
		*current = next
	}
	sort.Slice(state.Nodes, func(i, j int) bool { return state.Nodes[i].NodeID < state.Nodes[j].NodeID })
	if err := d.writeLocked(state); err != nil {
		return false, err
	}
	return false, nil
}

func (d *Directory) Lookup(identityHint string, now time.Time) ([]SignedAdvertisement, error) {
	if d == nil || now.IsZero() || len(identityHint) != len("gih_")+22 || !strings.HasPrefix(identityHint, "gih_") {
		return nil, ErrDirectoryInvalid
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	unlock, err := lockDirectory(d.root)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := d.readLocked()
	if errors.Is(err, os.ErrNotExist) {
		return []SignedAdvertisement{}, nil
	}
	if err != nil {
		return nil, err
	}
	results := make([]SignedAdvertisement, 0)
	for _, record := range state.Nodes {
		if record.Frozen || record.IdentityHint != identityHint || !record.ExpiresAt.After(now.UTC()) {
			continue
		}
		canonical, cErr := base64.RawURLEncoding.DecodeString(record.Canonical)
		signature, sErr := base64.RawURLEncoding.DecodeString(record.Signature)
		if cErr != nil || sErr != nil {
			return nil, ErrDirectoryInvalid
		}
		results = append(results, SignedAdvertisement{Canonical: canonical, Signature: signature, NodeID: record.NodeID, IdentityHint: record.IdentityHint, Sequence: record.Sequence, ExpiresAt: record.ExpiresAt})
		if len(results) > maxLookupResults {
			return nil, ErrLookupLimit
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].NodeID < results[j].NodeID })
	return results, nil
}

func storedFromVerified(verified discoveryprotocol.VerifiedAdvertisement) storedNode {
	return storedNode{
		NodeID:       verified.Advertisement.NodeID,
		IdentityHint: verified.Advertisement.GlobalIdentityHint,
		Sequence:     verified.Advertisement.Sequence,
		Canonical:    base64.RawURLEncoding.EncodeToString(verified.Canonical),
		Signature:    base64.RawURLEncoding.EncodeToString(verified.Signature),
		ExpiresAt:    mustParseTime(verified.Advertisement.ExpiresAt),
	}
}

func mustParseTime(value string) time.Time { t, _ := time.Parse(time.RFC3339, value); return t }

func compareSequence(a, b string) int {
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func (d *Directory) readLocked() (directoryState, error) {
	path := filepath.Join(d.root, directoryStateFile)
	info, err := os.Lstat(path)
	if err != nil {
		return directoryState{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != directoryFileMode {
		return directoryState{}, ErrDirectoryInvalid
	}
	f, err := os.Open(path)
	if err != nil {
		return directoryState{}, ErrDirectoryInvalid
	}
	opened, statErr := f.Stat()
	data, readErr := io.ReadAll(io.LimitReader(f, maxDirectoryBytes+1))
	closeErr := f.Close()
	if statErr != nil || !os.SameFile(info, opened) || readErr != nil || closeErr != nil || len(data) > maxDirectoryBytes {
		return directoryState{}, ErrDirectoryInvalid
	}
	var state directoryState
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&state) != nil || decoder.Decode(&struct{}{}) != io.EOF || state.SchemaVersion != 1 || len(state.Nodes) > maxDirectoryNodes {
		return directoryState{}, ErrDirectoryInvalid
	}
	lastNode := ""
	for _, record := range state.Nodes {
		if record.NodeID == "" || record.NodeID <= lastNode || record.Sequence == "" || record.ExpiresAt.IsZero() || record.IdentityHint == "" || record.Frozen && len(record.ForkEvidence) != 2 || !record.Frozen && len(record.ForkEvidence) != 0 {
			return directoryState{}, ErrDirectoryInvalid
		}
		canonical, cErr := base64.RawURLEncoding.DecodeString(record.Canonical)
		signature, sErr := base64.RawURLEncoding.DecodeString(record.Signature)
		issuedAt, timeErr := advertisementIssuedAt(canonical)
		if cErr != nil || sErr != nil || timeErr != nil {
			return directoryState{}, ErrDirectoryInvalid
		}
		verified, verifyErr := discoveryprotocol.Verify(canonical, signature, issuedAt)
		if verifyErr != nil || verified.Advertisement.NodeID != record.NodeID || verified.Advertisement.GlobalIdentityHint != record.IdentityHint || verified.Advertisement.Sequence != record.Sequence || verified.Advertisement.ExpiresAt != record.ExpiresAt.UTC().Format(time.RFC3339) {
			return directoryState{}, ErrDirectoryInvalid
		}
		if record.Frozen {
			for _, evidence := range record.ForkEvidence {
				forkCanonical, fcErr := base64.RawURLEncoding.DecodeString(evidence.Canonical)
				forkSignature, fsErr := base64.RawURLEncoding.DecodeString(evidence.Signature)
				forkIssued, fiErr := advertisementIssuedAt(forkCanonical)
				if fcErr != nil || fsErr != nil || fiErr != nil {
					return directoryState{}, ErrDirectoryInvalid
				}
				fork, ferr := discoveryprotocol.Verify(forkCanonical, forkSignature, forkIssued)
				if ferr != nil || fork.Advertisement.NodeID != record.NodeID || fork.Advertisement.Sequence != record.Sequence {
					return directoryState{}, ErrDirectoryInvalid
				}
			}
		}
		lastNode = record.NodeID
	}
	return state, nil
}

func advertisementIssuedAt(canonical []byte) (time.Time, error) {
	var ad struct {
		IssuedAt string `json:"issued_at"`
	}
	if json.Unmarshal(canonical, &ad) != nil {
		return time.Time{}, ErrDirectoryInvalid
	}
	return time.Parse(time.RFC3339, ad.IssuedAt)
}

func (d *Directory) writeLocked(state directoryState) error {
	data, err := json.Marshal(state)
	if err != nil || len(data) > maxDirectoryBytes {
		return ErrDirectoryInvalid
	}
	path := filepath.Join(d.root, directoryStateFile)
	tmp, err := os.CreateTemp(d.root, ".discovery-state-*.tmp")
	if err != nil {
		return errors.New("discovery state could not be written")
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(directoryFileMode); err != nil {
		_ = tmp.Close()
		return errors.New("discovery state permissions could not be set")
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return errors.New("discovery state could not be written")
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return errors.New("discovery state could not be synchronized")
	}
	if err := tmp.Close(); err != nil {
		return errors.New("discovery state could not be closed")
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return errors.New("discovery state could not be committed")
	}
	dir, err := os.Open(d.root)
	if err != nil {
		return errors.New("discovery directory could not be opened")
	}
	syncErr, closeErr := dir.Sync(), dir.Close()
	if syncErr != nil || closeErr != nil {
		return errors.New("discovery directory could not be synchronized")
	}
	return nil
}

type rateBucket struct {
	started time.Time
	count   int
}

// HTTPHandler exposes the bounded rendezvous storage API. It trusts no
// forwarded address headers, emits no CORS headers and never logs request
// bodies. Advertisement signature validity is checked before durable writes.
func HTTPHandler(directory *Directory) http.Handler {
	limiter := &requestLimiter{buckets: map[string]rateBucket{}}
	mux := http.NewServeMux()
	registerAuthorityHistoryRoutes(mux, directory, limiter)
	mux.HandleFunc("PUT /v1/identity-histories/{identityID}", func(w http.ResponseWriter, r *http.Request) {
		if directory == nil {
			http.Error(w, "directory unavailable", http.StatusServiceUnavailable)
			return
		}
		if !limiter.allow(r.RemoteAddr, time.Now()) {
			http.Error(w, "request unavailable", http.StatusTooManyRequests)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxHistoryBody)
		body, bodyErr := io.ReadAll(r.Body)
		if bodyErr != nil || len(body) == 0 || len(body) > maxHistoryBody {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if !strictJSONDocument(body) {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		var bundle globalidentity.HistoryBundle
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&bundle) != nil || decoder.Decode(&struct{}{}) != io.EOF || bundle.IdentityID != r.PathValue("identityID") {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		err := directory.PublishIdentityHistory(bundle)
		switch {
		case err == nil:
			w.WriteHeader(http.StatusOK)
		case errors.Is(err, globalidentity.ErrIdentityFork), errors.Is(err, globalidentity.ErrIdentityFrozen), errors.Is(err, globalidentity.ErrHistoryStale):
			http.Error(w, "identity history conflict", http.StatusConflict)
		case errors.Is(err, globalidentity.ErrHistoryInvalid), errors.Is(err, globalidentity.ErrHistoryDifferentID):
			http.Error(w, "invalid identity history", http.StatusBadRequest)
		default:
			http.Error(w, "directory unavailable", http.StatusServiceUnavailable)
		}
	})
	mux.HandleFunc("GET /v1/identity-histories/{identityID}", func(w http.ResponseWriter, r *http.Request) {
		if directory == nil {
			http.Error(w, "directory unavailable", http.StatusServiceUnavailable)
			return
		}
		if !limiter.allow(r.RemoteAddr, time.Now()) {
			http.Error(w, "request unavailable", http.StatusTooManyRequests)
			return
		}
		if r.ContentLength > 0 {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		bundle, found, err := directory.LookupIdentityHistory(r.PathValue("identityID"))
		if err != nil {
			http.Error(w, "directory unavailable", http.StatusServiceUnavailable)
			return
		}
		if !found {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(bundle)
	})
	mux.HandleFunc("POST /v1/advertisements", func(w http.ResponseWriter, r *http.Request) {
		if directory == nil {
			http.Error(w, "directory unavailable", http.StatusServiceUnavailable)
			return
		}
		if !limiter.allow(r.RemoteAddr, time.Now()) {
			http.Error(w, "request unavailable", http.StatusTooManyRequests)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxPublishBody)
		body, bodyErr := io.ReadAll(r.Body)
		if bodyErr != nil || len(body) == 0 || len(body) > maxPublishBody {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		if _, err := identityprotocol.Canonicalize(body); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		var request signedValue
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		canonical, cErr := base64.RawURLEncoding.DecodeString(request.Canonical)
		signature, sErr := base64.RawURLEncoding.DecodeString(request.Signature)
		if cErr != nil || sErr != nil || len(canonical) == 0 || len(canonical) > 32<<10 || len(signature) != 64 || base64.RawURLEncoding.EncodeToString(canonical) != request.Canonical || base64.RawURLEncoding.EncodeToString(signature) != request.Signature {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		alreadyPresent, err := directory.Publish(canonical, signature, time.Now().UTC())
		switch {
		case err == nil:
			if alreadyPresent {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusCreated)
			}
		case errors.Is(err, ErrEquivocation), errors.Is(err, ErrNodeFrozen), errors.Is(err, ErrDirectoryReplay):
			http.Error(w, "advertisement conflict", http.StatusConflict)
		case errors.Is(err, discoveryprotocol.ErrInvalid):
			http.Error(w, "invalid advertisement", http.StatusBadRequest)
		case errors.Is(err, ErrDirectoryFull):
			http.Error(w, "directory capacity unavailable", http.StatusServiceUnavailable)
		default:
			http.Error(w, "directory unavailable", http.StatusServiceUnavailable)
		}
	})
	mux.HandleFunc("GET /v1/advertisements", func(w http.ResponseWriter, r *http.Request) {
		if directory == nil {
			http.Error(w, "directory unavailable", http.StatusServiceUnavailable)
			return
		}
		if !limiter.allow(r.RemoteAddr, time.Now()) {
			http.Error(w, "request unavailable", http.StatusTooManyRequests)
			return
		}
		if r.ContentLength > maxLookupBody || r.URL.Query().Get("identity_hint") == "" {
			http.Error(w, "invalid query", http.StatusBadRequest)
			return
		}
		results, err := directory.Lookup(r.URL.Query().Get("identity_hint"), time.Now().UTC())
		if err != nil {
			http.Error(w, "directory unavailable", http.StatusServiceUnavailable)
			return
		}
		response := make([]signedValue, 0, len(results))
		for _, result := range results {
			response = append(response, signedValue{Canonical: base64.RawURLEncoding.EncodeToString(result.Canonical), Signature: base64.RawURLEncoding.EncodeToString(result.Signature)})
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"advertisements": response})
	})
	mux.HandleFunc("POST /v1/signals", func(w http.ResponseWriter, r *http.Request) {
		if directory == nil {
			http.Error(w, "directory unavailable", http.StatusServiceUnavailable)
			return
		}
		if !limiter.allow(r.RemoteAddr, time.Now()) {
			http.Error(w, "request unavailable", http.StatusTooManyRequests)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, signalingprotocol.MaxEnvelopeBytes)
		body, bodyErr := io.ReadAll(r.Body)
		if bodyErr != nil || len(body) == 0 || len(body) > signalingprotocol.MaxEnvelopeBytes {
			http.Error(w, "invalid signaling message", http.StatusBadRequest)
			return
		}
		message, err := signalingprotocol.ParseCanonicalMessage(body)
		if err != nil {
			http.Error(w, "invalid signaling message", http.StatusBadRequest)
			return
		}
		alreadyPresent, err := directory.PublishSignal(message, time.Now().UTC())
		switch {
		case err == nil:
			if alreadyPresent {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusCreated)
			}
		case errors.Is(err, signalingprotocol.ErrStale):
			http.Error(w, "expired signaling message", http.StatusBadRequest)
		case errors.Is(err, ErrSignalConflict):
			http.Error(w, "signaling message conflict", http.StatusConflict)
		case errors.Is(err, ErrSignalCapacity):
			http.Error(w, "signaling relay capacity unavailable", http.StatusServiceUnavailable)
		default:
			http.Error(w, "invalid signaling message", http.StatusBadRequest)
		}
	})
	mux.HandleFunc("GET /v1/signals", func(w http.ResponseWriter, r *http.Request) {
		if directory == nil {
			http.Error(w, "directory unavailable", http.StatusServiceUnavailable)
			return
		}
		if !limiter.allow(r.RemoteAddr, time.Now()) {
			http.Error(w, "request unavailable", http.StatusTooManyRequests)
			return
		}
		query := r.URL.Query()
		if r.ContentLength > 0 || len(query) != 1 || query.Get("target_node_id") == "" {
			http.Error(w, "invalid signaling query", http.StatusBadRequest)
			return
		}
		messages, err := directory.LookupSignals(query.Get("target_node_id"), time.Now().UTC())
		if err != nil {
			http.Error(w, "invalid signaling query", http.StatusBadRequest)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(signalLookupResponse{Messages: messages})
	})
	return mux
}

type requestLimiter struct {
	mu      sync.Mutex
	buckets map[string]rateBucket
}

func (l *requestLimiter) allow(remoteAddress string, now time.Time) bool {
	host, _, err := net.SplitHostPort(remoteAddress)
	if err != nil || host == "" {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.buckets) >= maxRateEntries {
		for key, bucket := range l.buckets {
			if now.Sub(bucket.started) > time.Minute {
				delete(l.buckets, key)
			}
		}
		if len(l.buckets) >= maxRateEntries {
			return false
		}
	}
	bucket := l.buckets[host]
	if now.Sub(bucket.started) >= time.Minute || bucket.started.IsZero() {
		bucket = rateBucket{started: now}
	}
	if bucket.count >= requestsPerMinute {
		l.buckets[host] = bucket
		return false
	}
	bucket.count++
	l.buckets[host] = bucket
	return true
}
