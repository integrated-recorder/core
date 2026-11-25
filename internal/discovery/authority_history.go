package discovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

const (
	authorityHistoryFile       = "owner-authority-histories-v1.json"
	maxAuthorityHistoryBody    = 16 << 20
	maxAuthorityHistoryEntries = 4096
	maxAuthorityHistoryHeads   = 8
)

var (
	identityIDWirePattern = regexp.MustCompile(`^gi_[A-Za-z0-9_-]{22}$`)
	subjectIDWirePattern  = regexp.MustCompile(`^os_[A-Za-z0-9_-]{22}$`)
	sequenceWirePattern   = regexp.MustCompile(`^[1-9][0-9]{0,8}$`)
	ErrAuthorityHistory   = errors.New("owner authority mirror data is invalid")
	ErrAuthorityHistLimit = errors.New("owner authority mirror capacity reached")
)

// OwnerAuthorityHistoryBundle is a transport-only, public history envelope.
// Discovery checks bounds and identifiers but intentionally does not decide
// signature validity or authority. Recorder nodes must validate every signed
// event against current Global Identity history before applying it.
type OwnerAuthorityHistoryBundle struct {
	Version    int               `json:"schema_version"`
	Identity   string            `json:"identity_id"`
	Subject    string            `json:"subject_id"`
	Events     []json.RawMessage `json:"events"`
	ForkEvents []json.RawMessage `json:"fork_events"`
}

type ownerAuthorityMirrorDisk struct {
	Version  int                          `json:"schema_version"`
	Subjects []ownerAuthorityMirrorRecord `json:"subjects"`
}

type ownerAuthorityMirrorRecord struct {
	Subject string            `json:"subject_id"`
	Heads   []json.RawMessage `json:"candidate_histories"`
}

// PublishOwnerAuthorityHistory stores a bounded public signed history from an
// untrusted caller. Prefixes are compacted; competing successors are retained
// as separate candidates so consumers can validate them and freeze on a real
// fork. The Discovery service never selects an authority branch.
func (d *Directory) PublishOwnerAuthorityHistory(subjectID string, body []byte) error {
	if d == nil || !subjectIDWirePattern.MatchString(subjectID) || len(body) == 0 || len(body) > maxAuthorityHistoryBody || !strictJSONDocument(body) {
		return ErrAuthorityHistory
	}
	incoming, canonical, err := decodeAuthorityHistoryBundle(body, subjectID)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	unlock, err := lockDirectory(d.root)
	if err != nil {
		return ErrDirectoryInvalid
	}
	defer unlock()
	disk, err := d.readAuthorityHistoryLocked()
	if errors.Is(err, os.ErrNotExist) {
		disk = ownerAuthorityMirrorDisk{Version: 1, Subjects: []ownerAuthorityMirrorRecord{}}
	} else if err != nil {
		return err
	}
	index := sort.Search(len(disk.Subjects), func(i int) bool { return disk.Subjects[i].Subject >= subjectID })
	if index == len(disk.Subjects) || disk.Subjects[index].Subject != subjectID {
		if len(disk.Subjects) >= maxDirectoryNodes {
			return ErrAuthorityHistLimit
		}
		disk.Subjects = append(disk.Subjects, ownerAuthorityMirrorRecord{})
		copy(disk.Subjects[index+1:], disk.Subjects[index:])
		disk.Subjects[index] = ownerAuthorityMirrorRecord{Subject: subjectID, Heads: []json.RawMessage{canonical}}
		return d.writeAuthorityHistoryLocked(disk)
	}
	record := &disk.Subjects[index]
	heads := append([]json.RawMessage(nil), record.Heads...)
	newHeads := make([]json.RawMessage, 0, len(heads)+1)
	mergedForks := append([]json.RawMessage(nil), incoming.ForkEvents...)
	compacted := false
	for _, existingRaw := range heads {
		existing, existingCanonical, decodeErr := decodeAuthorityHistoryBundle(existingRaw, subjectID)
		if decodeErr != nil {
			return ErrDirectoryInvalid
		}
		mergedForks = mergeRawEvents(mergedForks, existing.ForkEvents)
		switch {
		case equalRawEvents(existing.Events, incoming.Events):
			compacted = true
		case rawEventsPrefix(existing.Events, incoming.Events):
			compacted = true
		case rawEventsPrefix(incoming.Events, existing.Events):
			newHeads = append(newHeads, existingCanonical)
			compacted = true
		default:
			newHeads = append(newHeads, existingCanonical)
		}
	}
	if !compacted {
		newHeads = append(newHeads, canonical)
	} else {
		// Replace any prefix candidate with the longest incoming sequence when
		// incoming is longer, and merge signed fork witnesses without choosing
		// among candidate branches.
		keptIncoming := true
		for i := range newHeads {
			candidate, _, decodeErr := decodeAuthorityHistoryBundle(newHeads[i], subjectID)
			if decodeErr != nil {
				return ErrDirectoryInvalid
			}
			if rawEventsPrefix(candidate.Events, incoming.Events) || rawEventsPrefix(incoming.Events, candidate.Events) {
				if len(incoming.Events) > len(candidate.Events) {
					newHeads[i] = canonical
				}
				keptIncoming = false
				break
			}
		}
		if keptIncoming {
			newHeads = append(newHeads, canonical)
		}
	}
	// Carry all explicitly signed fork witnesses into every candidate. This
	// cannot make an invalid branch authoritative: each recipient verifies the
	// event signatures and commits fork evidence locally before denying use.
	for i := range newHeads {
		candidate, _, decodeErr := decodeAuthorityHistoryBundle(newHeads[i], subjectID)
		if decodeErr != nil {
			return ErrDirectoryInvalid
		}
		candidate.ForkEvents = mergeRawEvents(candidate.ForkEvents, mergedForks)
		newHeads[i], err = json.Marshal(candidate)
		if err != nil {
			return ErrAuthorityHistory
		}
	}
	if len(newHeads) > maxAuthorityHistoryHeads {
		return ErrAuthorityHistLimit
	}
	if len(newHeads) == 0 {
		candidate := incoming
		candidate.ForkEvents = mergedForks
		encoded, marshalErr := json.Marshal(candidate)
		if marshalErr != nil {
			return ErrAuthorityHistory
		}
		newHeads = append(newHeads, encoded)
	}
	if totalRawBytes(newHeads) > maxAuthorityHistoryBody {
		return ErrAuthorityHistLimit
	}
	sort.Slice(newHeads, func(i, j int) bool { return rawDigest(newHeads[i]) < rawDigest(newHeads[j]) })
	record.Heads = newHeads
	return d.writeAuthorityHistoryLocked(disk)
}

// LookupOwnerAuthorityHistories returns every retained candidate. It has no
// cache/freshness semantics; a federation client must compare every
// configured HTTPS provider on each remote authorization decision.
func (d *Directory) LookupOwnerAuthorityHistories(subjectID string) ([][]byte, error) {
	if d == nil || !subjectIDWirePattern.MatchString(subjectID) {
		return nil, ErrAuthorityHistory
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	unlock, err := lockDirectory(d.root)
	if err != nil {
		return nil, ErrDirectoryInvalid
	}
	defer unlock()
	disk, err := d.readAuthorityHistoryLocked()
	if errors.Is(err, os.ErrNotExist) {
		return [][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}
	index := sort.Search(len(disk.Subjects), func(i int) bool { return disk.Subjects[i].Subject >= subjectID })
	if index == len(disk.Subjects) || disk.Subjects[index].Subject != subjectID {
		return [][]byte{}, nil
	}
	result := make([][]byte, len(disk.Subjects[index].Heads))
	for i, head := range disk.Subjects[index].Heads {
		result[i] = append([]byte(nil), head...)
	}
	return result, nil
}

func decodeAuthorityHistoryBundle(body []byte, subjectID string) (OwnerAuthorityHistoryBundle, json.RawMessage, error) {
	var bundle OwnerAuthorityHistoryBundle
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&bundle) != nil || decoder.Decode(&struct{}{}) != io.EOF || bundle.Version != 1 || !identityIDWirePattern.MatchString(bundle.Identity) || bundle.Subject != subjectID || len(bundle.Events) == 0 || len(bundle.Events) > maxAuthorityHistoryEntries || len(bundle.ForkEvents) > 16 {
		return OwnerAuthorityHistoryBundle{}, nil, ErrAuthorityHistory
	}
	for _, event := range append(append([]json.RawMessage(nil), bundle.Events...), bundle.ForkEvents...) {
		if !validAuthorityEventEnvelope(event, bundle.Identity, bundle.Subject) {
			return OwnerAuthorityHistoryBundle{}, nil, ErrAuthorityHistory
		}
	}
	canonical, err := json.Marshal(bundle)
	if err != nil || len(canonical) > maxAuthorityHistoryBody {
		return OwnerAuthorityHistoryBundle{}, nil, ErrAuthorityHistory
	}
	return bundle, canonical, nil
}

func validAuthorityEventEnvelope(raw []byte, identityID, subjectID string) bool {
	if !strictJSONDocument(raw) {
		return false
	}
	var event struct {
		Event struct {
			Version      string `json:"event_version"`
			IdentityID   string `json:"identity_id"`
			IdentityHead string `json:"identity_head_hash"`
			SubjectID    string `json:"subject_id"`
			Sequence     string `json:"sequence"`
			PreviousHash string `json:"previous_event_hash,omitempty"`
			Operation    string `json:"operation"`
			IssuedAt     string `json:"issued_at"`
			Signer       struct {
				Kind string `json:"kind"`
				ID   string `json:"id"`
			} `json:"signer"`
			SignerKeyID string          `json:"signer_key_id"`
			Payload     json.RawMessage `json:"payload"`
		} `json:"event"`
		Signature string `json:"signature"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&event) != nil || decoder.Decode(&struct{}{}) != io.EOF || event.Event.Version == "" ||
		event.Event.IdentityID != identityID || event.Event.SubjectID != subjectID ||
		!sequenceWirePattern.MatchString(event.Event.Sequence) || event.Event.Signer.ID == "" ||
		event.Event.Signer.Kind == "" || event.Event.SignerKeyID == "" || len(event.Event.Payload) == 0 {
		return false
	}
	if _, err := strconv.ParseUint(event.Event.Sequence, 10, 32); err != nil {
		return false
	}
	signature, err := base64.StdEncoding.DecodeString(event.Signature)
	return err == nil && len(signature) == 64
}

func (d *Directory) readAuthorityHistoryLocked() (ownerAuthorityMirrorDisk, error) {
	path := filepath.Join(d.root, authorityHistoryFile)
	info, err := os.Lstat(path)
	if err != nil {
		return ownerAuthorityMirrorDisk{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != directoryFileMode {
		return ownerAuthorityMirrorDisk{}, ErrDirectoryInvalid
	}
	f, err := os.Open(path)
	if err != nil {
		return ownerAuthorityMirrorDisk{}, ErrDirectoryInvalid
	}
	opened, statErr := f.Stat()
	body, readErr := io.ReadAll(io.LimitReader(f, maxDirectoryBytes+1))
	closeErr := f.Close()
	if statErr != nil || !os.SameFile(info, opened) || readErr != nil || closeErr != nil || len(body) > maxDirectoryBytes || !strictJSONDocument(body) {
		return ownerAuthorityMirrorDisk{}, ErrDirectoryInvalid
	}
	var disk ownerAuthorityMirrorDisk
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&disk) != nil || decoder.Decode(&struct{}{}) != io.EOF || disk.Version != 1 || len(disk.Subjects) > maxDirectoryNodes {
		return ownerAuthorityMirrorDisk{}, ErrDirectoryInvalid
	}
	last := ""
	for _, record := range disk.Subjects {
		if !subjectIDWirePattern.MatchString(record.Subject) || record.Subject <= last || len(record.Heads) == 0 || len(record.Heads) > maxAuthorityHistoryHeads || totalRawBytes(record.Heads) > maxAuthorityHistoryBody {
			return ownerAuthorityMirrorDisk{}, ErrDirectoryInvalid
		}
		last = record.Subject
		for _, head := range record.Heads {
			if _, _, err := decodeAuthorityHistoryBundle(head, record.Subject); err != nil {
				return ownerAuthorityMirrorDisk{}, ErrDirectoryInvalid
			}
		}
	}
	return disk, nil
}

func (d *Directory) writeAuthorityHistoryLocked(disk ownerAuthorityMirrorDisk) error {
	body, err := json.Marshal(disk)
	if err != nil || len(body) > maxDirectoryBytes {
		return ErrDirectoryInvalid
	}
	path := filepath.Join(d.root, authorityHistoryFile)
	tmp, err := os.CreateTemp(d.root, ".owner-authority-*.tmp")
	if err != nil {
		return ErrDirectoryInvalid
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(directoryFileMode); err != nil {
		_ = tmp.Close()
		return ErrDirectoryInvalid
	}
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		return ErrDirectoryInvalid
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return ErrDirectoryInvalid
	}
	if err := tmp.Close(); err != nil || os.Rename(tmpPath, path) != nil {
		return ErrDirectoryInvalid
	}
	dir, err := os.Open(d.root)
	if err != nil {
		return ErrDirectoryInvalid
	}
	syncErr, closeErr := dir.Sync(), dir.Close()
	if syncErr != nil || closeErr != nil {
		return ErrDirectoryInvalid
	}
	return nil
}

func mergeRawEvents(left, right []json.RawMessage) []json.RawMessage {
	seen := make(map[string]json.RawMessage, len(left)+len(right))
	for _, event := range append(append([]json.RawMessage(nil), left...), right...) {
		var compact bytes.Buffer
		if json.Compact(&compact, event) == nil {
			seen[compact.String()] = append(json.RawMessage(nil), compact.Bytes()...)
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]json.RawMessage, 0, len(keys))
	for _, key := range keys {
		result = append(result, seen[key])
	}
	return result
}

func equalRawEvents(a, b []json.RawMessage) bool {
	return len(a) == len(b) && rawEventsPrefix(a, b)
}

func rawEventsPrefix(prefix, events []json.RawMessage) bool {
	if len(prefix) > len(events) {
		return false
	}
	for i := range prefix {
		var left, right bytes.Buffer
		if json.Compact(&left, prefix[i]) != nil || json.Compact(&right, events[i]) != nil || !bytes.Equal(left.Bytes(), right.Bytes()) {
			return false
		}
	}
	return true
}

func totalRawBytes(values []json.RawMessage) int {
	total := 0
	for _, value := range values {
		total += len(value)
	}
	return total
}

func rawDigest(body []byte) string {
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}

// registerAuthorityHistoryRoutes adds the opaque mirror endpoints to the
// directory API. Only bounded public signed history is accepted; credentials,
// API keys and HTTP request bodies are never logged.
func registerAuthorityHistoryRoutes(mux *http.ServeMux, directory *Directory, limiter *requestLimiter) {
	mux.HandleFunc("PUT /v1/owner-authority-histories/{subjectID}", func(w http.ResponseWriter, r *http.Request) {
		if directory == nil || !limiter.allow(r.RemoteAddr, time.Now()) {
			http.Error(w, "request unavailable", http.StatusTooManyRequests)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxAuthorityHistoryBody)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		err = directory.PublishOwnerAuthorityHistory(r.PathValue("subjectID"), body)
		switch {
		case err == nil:
			w.WriteHeader(http.StatusOK)
		case errors.Is(err, ErrAuthorityHistLimit):
			http.Error(w, "history capacity reached", http.StatusRequestEntityTooLarge)
		case errors.Is(err, ErrDirectoryInvalid):
			http.Error(w, "directory unavailable", http.StatusServiceUnavailable)
		default:
			http.Error(w, "invalid authority history", http.StatusBadRequest)
		}
	})
	mux.HandleFunc("GET /v1/owner-authority-histories/{subjectID}", func(w http.ResponseWriter, r *http.Request) {
		if directory == nil || !limiter.allow(r.RemoteAddr, time.Now()) {
			http.Error(w, "request unavailable", http.StatusTooManyRequests)
			return
		}
		if r.ContentLength > 0 {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		heads, err := directory.LookupOwnerAuthorityHistories(r.PathValue("subjectID"))
		if err != nil {
			http.Error(w, "directory unavailable", http.StatusServiceUnavailable)
			return
		}
		if len(heads) == 0 {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"histories": [`))
		for i, head := range heads {
			if i > 0 {
				_, _ = w.Write([]byte{','})
			}
			_, _ = w.Write(head)
		}
		_, _ = w.Write([]byte(`]}`))
	})
}

// PublishOwnerAuthorityHistory sends a signed history mirror to every
// configured HTTPS provider. Success is withheld unless every provider has
// acknowledged the same publication attempt.
func (c *FederationClient) PublishOwnerAuthorityHistory(ctx context.Context, subjectID string, bundle OwnerAuthorityHistoryBundle) ([]ProviderStatus, error) {
	if c == nil || ctx == nil || !subjectIDWirePattern.MatchString(subjectID) || bundle.Subject != subjectID {
		return nil, ErrFederationConfig
	}
	body, err := json.Marshal(bundle)
	if err != nil || len(body) == 0 || len(body) > maxAuthorityHistoryBody {
		return nil, ErrAuthorityHistory
	}
	if _, _, err := decodeAuthorityHistoryBundle(body, subjectID); err != nil {
		return nil, err
	}
	statuses := c.callAll(func(provider providerEndpoint) ProviderStatus {
		endpoint := *provider.base
		endpoint.Path = "/v1/owner-authority-histories/" + url.PathEscape(subjectID)
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint.String(), bytes.NewReader(body))
		if err != nil {
			return ProviderStatus{Host: provider.host, Code: "request_invalid"}
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := c.client.Do(request)
		if err != nil {
			return ProviderStatus{Host: provider.host, Code: "transport_failed"}
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return ProviderStatus{Host: provider.host, Code: "http_status"}
		}
		return ProviderStatus{Host: provider.host, Success: true}
	})
	for _, status := range statuses {
		if !status.Success {
			return statuses, ErrAuthorityIncomplete
		}
	}
	return statuses, nil
}

type FederatedAuthorityHistoryLookup struct {
	Histories [][]byte
	Providers []ProviderStatus
	Complete  bool
	Agreed    bool
}

// LookupOwnerAuthorityHistories fetches every configured provider with no
// cache and requires byte-equivalent candidate sets. Returned candidates are
// opaque; the caller must cryptographically verify and apply them.
func (c *FederationClient) LookupOwnerAuthorityHistories(ctx context.Context, subjectID string) (FederatedAuthorityHistoryLookup, error) {
	if c == nil || ctx == nil || !subjectIDWirePattern.MatchString(subjectID) {
		return FederatedAuthorityHistoryLookup{}, ErrFederationConfig
	}
	type responseValue struct {
		status ProviderStatus
		heads  [][]byte
		wire   []byte
	}
	responses := make(chan responseValue, len(c.providers))
	for _, provider := range c.providers {
		provider := provider
		go func() {
			endpoint := *provider.base
			endpoint.Path = "/v1/owner-authority-histories/" + url.PathEscape(subjectID)
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
			if err != nil {
				responses <- responseValue{status: ProviderStatus{Host: provider.host, Code: "request_invalid"}}
				return
			}
			request.Header.Set("Accept", "application/json")
			response, err := c.client.Do(request)
			if err != nil {
				responses <- responseValue{status: ProviderStatus{Host: provider.host, Code: "transport_failed"}}
				return
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				responses <- responseValue{status: ProviderStatus{Host: provider.host, Code: "http_status"}}
				return
			}
			body, err := io.ReadAll(io.LimitReader(response.Body, maxAuthorityHistoryBody+1))
			if err != nil || len(body) == 0 || len(body) > maxAuthorityHistoryBody || !strictJSONDocument(body) {
				responses <- responseValue{status: ProviderStatus{Host: provider.host, Code: "response_invalid"}}
				return
			}
			var payload struct {
				Histories []json.RawMessage `json:"histories"`
			}
			decoder := json.NewDecoder(bytes.NewReader(body))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&payload) != nil || decoder.Decode(&struct{}{}) != io.EOF || len(payload.Histories) == 0 || len(payload.Histories) > maxAuthorityHistoryHeads {
				responses <- responseValue{status: ProviderStatus{Host: provider.host, Code: "response_invalid"}}
				return
			}
			heads := make([][]byte, 0, len(payload.Histories))
			for _, raw := range payload.Histories {
				_, canonical, decodeErr := decodeAuthorityHistoryBundle(raw, subjectID)
				if decodeErr != nil {
					responses <- responseValue{status: ProviderStatus{Host: provider.host, Code: "history_invalid"}}
					return
				}
				heads = append(heads, canonical)
			}
			canonical, err := json.Marshal(struct {
				Histories [][]byte `json:"histories"`
			}{Histories: heads})
			if err != nil {
				responses <- responseValue{status: ProviderStatus{Host: provider.host, Code: "response_invalid"}}
				return
			}
			responses <- responseValue{status: ProviderStatus{Host: provider.host, Success: true}, heads: heads, wire: canonical}
		}()
	}
	result := FederatedAuthorityHistoryLookup{Providers: make([]ProviderStatus, 0, len(c.providers))}
	var agreed []byte
	disagreed := false
	allHeads := make(map[string][]byte)
	for range c.providers {
		response := <-responses
		result.Providers = append(result.Providers, response.status)
		if !response.status.Success {
			continue
		}
		for _, head := range response.heads {
			allHeads[rawDigest(head)] = append([]byte(nil), head...)
		}
		if agreed == nil {
			agreed = response.wire
		} else if !bytes.Equal(agreed, response.wire) {
			disagreed = true
		}
	}
	for _, head := range allHeads {
		result.Histories = append(result.Histories, head)
	}
	sort.Slice(result.Histories, func(i, j int) bool { return rawDigest(result.Histories[i]) < rawDigest(result.Histories[j]) })
	sort.Slice(result.Providers, func(i, j int) bool { return result.Providers[i].Host < result.Providers[j].Host })
	result.Complete = len(result.Providers) == len(c.providers)
	for _, provider := range result.Providers {
		if !provider.Success {
			result.Complete = false
		}
	}
	result.Agreed = result.Complete && agreed != nil && !disagreed
	if !result.Complete {
		return result, ErrNoProvider
	}
	if !result.Agreed {
		return result, ErrAuthorityDisagrees
	}
	return result, nil
}

var (
	ErrAuthorityIncomplete = errors.New("owner authority history was not published to every provider")
	ErrAuthorityDisagrees  = errors.New("configured providers disagree on owner authority history")
)
