package storageproto

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MaxConcurrentReadStreams = 16

type ServeOptions struct {
	SocketPath string
	TokenFile  string
	// MaxConcurrentReads opts a provider into overlapping object GET/range
	// streams. All other operations remain exclusive with reads and each other.
	// Zero preserves the original fully serialized request behavior.
	MaxConcurrentReads int
	// ParentLiveness is a pipe owned by the supervising parent. EOF is a
	// cancellation boundary: Serve closes active requests and exits. The
	// provider executable should leave this nil or pass os.Stdin, which Start
	// reserves for this purpose. It carries no protocol data.
	ParentLiveness io.Reader
}

func Serve(ctx context.Context, provider Provider, options ServeOptions) error {
	if ctx == nil || ctx.Err() != nil || provider == nil || !filepath.IsAbs(options.SocketPath) || !filepath.IsAbs(options.TokenFile) || !validConcurrentReadLimit(options.MaxConcurrentReads) {
		return fmt.Errorf("storage provider server configuration is invalid")
	}
	descriptor := provider.Descriptor()
	if err := ValidateDescriptor(descriptor); err != nil {
		return err
	}
	token, err := readToken(options.TokenFile)
	if err != nil {
		return err
	}
	defer func() {
		for i := range token {
			token[i] = 0
		}
	}()
	if err = os.MkdirAll(filepath.Dir(options.SocketPath), 0700); err != nil {
		return fmt.Errorf("create private socket directory: %w", err)
	}
	if _, err = os.Lstat(options.SocketPath); err == nil {
		return fmt.Errorf("storage provider socket path already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect storage provider socket path: %w", err)
	}
	listener, err := net.Listen("unix", options.SocketPath)
	if err != nil {
		return fmt.Errorf("listen on storage provider socket: %w", err)
	}
	if err = os.Chmod(options.SocketPath, 0600); err != nil {
		_ = listener.Close()
		_ = os.Remove(options.SocketPath)
		return fmt.Errorf("protect storage provider socket: %w", err)
	}
	server := &http.Server{
		Handler:           providerHandler(provider, token, options.MaxConcurrentReads),
		ReadHeaderTimeout: 5 * time.Second,
		MaxHeaderBytes:    MaxControlFrameBytes,
		ErrorLog:          log.New(io.Discard, "", 0),
	}
	stopped := make(chan struct{})
	parentGone := make(chan struct{})
	parentLiveness := options.ParentLiveness
	if parentLiveness == nil {
		parentLiveness = os.Stdin
	}
	if parentLiveness != nil {
		go func() {
			_, _ = io.Copy(io.Discard, parentLiveness)
			close(parentGone)
		}()
	}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
				// A provider that ignores cancellation must not outlive its server
				// lifecycle indefinitely.
				_ = server.Close()
			}
			cancel()
		case <-parentGone:
			// Parent death is a hard cancellation boundary. Close active UDS
			// connections so request contexts (including streaming Put) cancel.
			_ = server.Close()
		case <-stopped:
		}
	}()
	err = server.Serve(listener)
	close(stopped)
	<-shutdownDone
	if closer, ok := parentLiveness.(io.Closer); ok {
		_ = closer.Close()
	}
	_ = os.Remove(options.SocketPath)
	if errors.Is(err, http.ErrServerClosed) || ctx.Err() != nil {
		return nil
	}
	return err
}

func providerHandler(provider Provider, token []byte, maxConcurrentReads int) http.Handler {
	mux := http.NewServeMux()
	requestGate := newProviderRequestGate(maxConcurrentReads)
	descriptor := provider.Descriptor()
	mux.HandleFunc("/v1/descriptor", func(w http.ResponseWriter, r *http.Request) {
		if !method(w, r, http.MethodGet) {
			return
		}
		if !queryOnly(r, "") {
			writeError(w, http.StatusBadRequest, "invalid_request", "request is invalid")
			return
		}
		if !noBody(r) {
			writeError(w, http.StatusBadRequest, "invalid_request", "request is invalid")
			return
		}
		writeJSON(w, http.StatusOK, descriptor, MaxControlFrameBytes)
	})
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		if !method(w, r, http.MethodPut) {
			return
		}
		if !queryOnly(r, "") || !isJSONContentType(r.Header.Get("Content-Type")) {
			writeError(w, http.StatusBadRequest, "invalid_request", "request is invalid")
			return
		}
		var config Config
		if !decodeBody(w, r, MaxConfigBytes, &config) {
			return
		}
		if err := ValidateConfig(config); err != nil || ValidateConfigForSchema(descriptor.ConfigurationSchema, config) != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "request is invalid")
			return
		}
		if err := provider.Configure(r.Context(), config); err != nil {
			writeProviderError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/v1/probe", func(w http.ResponseWriter, r *http.Request) {
		if !method(w, r, http.MethodPost) {
			return
		}
		if !queryOnly(r, "") || !emptyJSONBody(w, r) {
			writeError(w, http.StatusBadRequest, "invalid_request", "request is invalid")
			return
		}
		if err := provider.Probe(r.Context()); err != nil {
			writeProviderError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"ready": true}, MaxControlFrameBytes)
	})
	mux.HandleFunc("/v1/objects", func(w http.ResponseWriter, r *http.Request) {
		key, ok := oneQuery(r, "key")
		if !queryOnly(r, "key") || !ok || ValidateKey(key) != nil {
			writeError(w, http.StatusBadRequest, "invalid_key", "object key is invalid")
			return
		}
		switch r.Method {
		case http.MethodPut:
			if r.ContentLength < 0 || r.ContentLength > MaxObjectBytes {
				writeError(w, http.StatusRequestEntityTooLarge, "invalid_size", "object size is invalid")
				return
			}
			counted := &countReader{r: r.Body, h: sha256.New()}
			info, err := provider.Put(r.Context(), key, counted, r.ContentLength)
			if err != nil {
				writeProviderError(w, err)
				return
			}
			if counted.n != r.ContentLength || info.Size != r.ContentLength || !validDigest(info.SHA256) || info.SHA256 != hex.EncodeToString(counted.h.Sum(nil)) {
				writeError(w, http.StatusBadRequest, "size_mismatch", "object size is invalid")
				return
			}
			writeJSON(w, http.StatusOK, info, MaxControlFrameBytes)
		case http.MethodHead:
			info, err := provider.Stat(r.Context(), key)
			if err != nil {
				writeProviderError(w, err)
				return
			}
			if !writeObjectHeaders(w, info) {
				writeError(w, http.StatusBadGateway, "invalid_provider_response", "provider response is invalid")
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if value := r.Header.Get("Range"); value != "" {
				offset, length, err := parseRange(value)
				if err != nil || offset > MaxObjectBytes-length {
					writeError(w, http.StatusRequestedRangeNotSatisfiable, "invalid_range", "requested range is invalid")
					return
				}
				reader, info, err := provider.OpenRange(r.Context(), key, offset, length)
				if err != nil {
					writeProviderError(w, err)
					return
				}
				defer reader.Close()
				if info.Size < 0 || info.Size > MaxObjectBytes || !validDigest(info.SHA256) || offset+length > info.Size {
					writeError(w, http.StatusBadGateway, "invalid_provider_response", "provider response is invalid")
					return
				}
				_ = writeObjectHeaders(w, info)
				w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, info.Size))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.Copy(w, io.LimitReader(reader, length))
				return
			}
			reader, info, err := provider.Open(r.Context(), key)
			if err != nil {
				writeProviderError(w, err)
				return
			}
			defer reader.Close()
			if !writeObjectHeaders(w, info) {
				writeError(w, http.StatusBadGateway, "invalid_provider_response", "provider response is invalid")
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = io.Copy(w, io.LimitReader(reader, info.Size))
		case http.MethodDelete:
			if !noBody(r) {
				writeError(w, http.StatusBadRequest, "invalid_request", "request is invalid")
				return
			}
			if err := provider.Delete(r.Context(), key); err != nil {
				writeProviderError(w, err)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method is not supported")
		}
	})
	mux.HandleFunc("/v1/list", func(w http.ResponseWriter, r *http.Request) {
		if !method(w, r, http.MethodGet) {
			return
		}
		if !queryOnly(r, "prefix", "cursor", "limit") {
			writeError(w, http.StatusBadRequest, "invalid_request", "request is invalid")
			return
		}
		prefix, ok := optionalOneQuery(r, "prefix")
		if !ok || ValidatePrefix(prefix) != nil {
			writeError(w, http.StatusBadRequest, "invalid_prefix", "object prefix is invalid")
			return
		}
		cursor, ok := optionalOneQuery(r, "cursor")
		if !ok || len(cursor) > 1024 {
			writeError(w, http.StatusBadRequest, "invalid_cursor", "list cursor is invalid")
			return
		}
		limitRaw, ok := oneQuery(r, "limit")
		limit, err := strconv.Atoi(limitRaw)
		if !ok || err != nil || limit < 1 || limit > MaxListLimit {
			writeError(w, http.StatusBadRequest, "invalid_limit", "list limit is invalid")
			return
		}
		page, err := provider.List(r.Context(), prefix, cursor, limit)
		if err != nil {
			writeProviderError(w, err)
			return
		}
		if err = validatePage(page, prefix, cursor, limit); err != nil {
			writeError(w, http.StatusBadGateway, "invalid_provider_response", "provider response is invalid")
			return
		}
		encoded, err := marshalBoundedListPage(page)
		if err != nil {
			writeError(w, http.StatusBadGateway, "invalid_provider_response", "provider response is invalid")
			return
		}
		writeJSONBytes(w, http.StatusOK, encoded, MaxControlFrameBytes)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !authOK(r.Header.Get("Authorization"), token) {
			writeError(w, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		switch r.URL.Path {
		case "/v1/descriptor", "/v1/config", "/v1/probe", "/v1/objects", "/v1/list":
		default:
			writeError(w, http.StatusNotFound, "not_found", "route not found")
			return
		}
		readRequest := concurrentObjectRead(maxConcurrentReads, r.Method, r.URL.Path)
		release, err := requestGate.acquire(r.Context(), readRequest)
		if err != nil {
			writeProviderError(w, r.Context().Err())
			return
		}
		defer release()
		mux.ServeHTTP(w, r)
	})
}

func concurrentObjectRead(maxConcurrentReads int, method, path string) bool {
	return maxConcurrentReads > 0 && path == "/v1/objects" && method == http.MethodGet
}

func validConcurrentReadLimit(limit int) bool {
	return limit >= 0 && limit <= MaxConcurrentReadStreams
}

// providerRequestGate allows a bounded number of read streams to share the
// provider while mutations/control operations take an exclusive turn. Waiting
// writers prevent new reads from entering, so an endless playback workload
// cannot starve object publication or control operations. A configured zero
// is represented as one reader and therefore preserves full serialization.
type providerRequestGate struct {
	mu             sync.Mutex
	changed        chan struct{}
	readers        int
	waitingWriters int
	writer         bool
	maxReaders     int
}

func newProviderRequestGate(maxReaders int) *providerRequestGate {
	if maxReaders == 0 {
		maxReaders = 1
	}
	return &providerRequestGate{changed: make(chan struct{}), maxReaders: maxReaders}
}

func (g *providerRequestGate) acquire(ctx context.Context, read bool) (func(), error) {
	if ctx == nil {
		return nil, errors.New("storage provider request context is unavailable")
	}
	if read {
		return g.acquireRead(ctx)
	}
	return g.acquireWrite(ctx)
}

func (g *providerRequestGate) acquireRead(ctx context.Context) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		g.mu.Lock()
		if !g.writer && g.waitingWriters == 0 && g.readers < g.maxReaders {
			g.readers++
			g.signalLocked()
			g.mu.Unlock()
			return g.releaseRead, nil
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (g *providerRequestGate) acquireWrite(ctx context.Context) (func(), error) {
	g.mu.Lock()
	if err := ctx.Err(); err != nil {
		g.mu.Unlock()
		return nil, err
	}
	g.waitingWriters++
	g.signalLocked()
	for {
		if err := ctx.Err(); err != nil {
			g.waitingWriters--
			g.signalLocked()
			g.mu.Unlock()
			return nil, err
		}
		if !g.writer && g.readers == 0 {
			g.waitingWriters--
			g.writer = true
			g.signalLocked()
			g.mu.Unlock()
			return g.releaseWrite, nil
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		g.mu.Lock()
	}
}

func (g *providerRequestGate) releaseRead() {
	g.mu.Lock()
	g.readers--
	g.signalLocked()
	g.mu.Unlock()
}

func (g *providerRequestGate) releaseWrite() {
	g.mu.Lock()
	g.writer = false
	g.signalLocked()
	g.mu.Unlock()
}

func (g *providerRequestGate) signalLocked() {
	close(g.changed)
	g.changed = make(chan struct{})
}

type countReader struct {
	r io.Reader
	n int64
	h hash.Hash
}

func (r *countReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	if n > 0 {
		_, _ = r.h.Write(p[:n])
	}
	return n, err
}

func readToken(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 {
		return nil, fmt.Errorf("storage provider token file is invalid")
	}
	b, err := os.ReadFile(path)
	if err != nil || len(b) < 32 || len(b) > 256 || strings.ContainsAny(string(b), "\r\n\x00") {
		return nil, fmt.Errorf("storage provider token file is invalid")
	}
	for _, value := range b {
		if !(value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z' || value >= '0' && value <= '9' || strings.ContainsRune("-_.~", rune(value))) {
			return nil, fmt.Errorf("storage provider token file is invalid")
		}
	}
	return b, nil
}

func authOK(header string, token []byte) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	provided := []byte(strings.TrimPrefix(header, prefix))
	if len(provided) != len(token) {
		return false
	}
	return subtle.ConstantTimeCompare(provided, token) == 1
}

func method(w http.ResponseWriter, r *http.Request, expected string) bool {
	if r.Method != expected {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method is not supported")
		return false
	}
	return true
}
func isJSONContentType(value string) bool {
	mediaType := strings.TrimSpace(strings.SplitN(value, ";", 2)[0])
	return strings.EqualFold(mediaType, "application/json")
}
func noBody(r *http.Request) bool { return r.ContentLength == 0 }
func emptyJSONBody(w http.ResponseWriter, r *http.Request) bool {
	var body struct{}
	return r.ContentLength == 2 && decodeBody(w, r, 2, &body)
}

func decodeBody(w http.ResponseWriter, r *http.Request, maximum int64, target any) bool {
	if r.ContentLength < 0 || r.ContentLength > maximum {
		writeError(w, http.StatusRequestEntityTooLarge, "invalid_size", "request size is invalid")
		return false
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maximum))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "request is invalid")
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_request", "request is invalid")
		return false
	}
	return true
}

func ValidateConfig(config Config) error {
	if len(config.Values) > 64 || len(config.Secrets) > 64 || len(config.Values)+len(config.Secrets) > 64 {
		return ErrProtocol
	}
	for key, value := range config.Values {
		if !configKeyPattern.MatchString(key) || len(value) == 0 || len(value) > 16<<10 || !json.Valid(value) {
			return ErrProtocol
		}
	}
	for key, value := range config.Secrets {
		if !configKeyPattern.MatchString(key) || len(value) > 16<<10 || !validText(value, 0, 16<<10) {
			return ErrProtocol
		}
	}
	return nil
}

func ValidateConfigForSchema(schema Schema, config Config) error {
	fields := make(map[string]Field, len(schema.Fields))
	for _, field := range schema.Fields {
		fields[field.Key] = field
	}
	for key := range config.Values {
		field, ok := fields[key]
		if !ok || field.Control == "secret" {
			return ErrProtocol
		}
	}
	for key := range config.Secrets {
		field, ok := fields[key]
		if !ok || field.Control != "secret" {
			return ErrProtocol
		}
	}
	for _, field := range schema.Fields {
		var raw json.RawMessage
		if field.Control == "secret" {
			value, present := config.Secrets[field.Key]
			if field.Required && (!present || value == "") {
				return ErrProtocol
			}
			if present && field.Constraints != nil && (field.Constraints.MinLength != nil && len(value) < *field.Constraints.MinLength || field.Constraints.MaxLength != nil && len(value) > *field.Constraints.MaxLength) {
				return ErrProtocol
			}
			continue
		}
		raw, _ = config.Values[field.Key]
		if len(raw) == 0 {
			if field.Required && len(field.Default) == 0 {
				return ErrProtocol
			}
			continue
		}
		if !validSchemaValue(field, raw) {
			return ErrProtocol
		}
	}
	return nil
}

func writeProviderError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "object not found")
	case errors.Is(err, ErrInvalidKey):
		writeError(w, http.StatusBadRequest, "invalid_key", "object key is invalid")
	case errors.Is(err, ErrInvalidRange):
		writeError(w, http.StatusRequestedRangeNotSatisfiable, "invalid_range", "requested range is invalid")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, "canceled", "operation canceled")
	case errors.Is(err, ErrUnsupported):
		writeError(w, http.StatusNotImplemented, "unsupported", "operation is unsupported")
	default:
		writeError(w, http.StatusBadGateway, "provider_error", "storage provider operation failed")
	}
}

type wireError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}}, MaxControlFrameBytes)
}
func writeJSON(w http.ResponseWriter, status int, value any, maximum int) {
	b, err := json.Marshal(value)
	if err != nil {
		http.Error(w, "provider response invalid", http.StatusBadGateway)
		return
	}
	writeJSONBytes(w, status, b, maximum)
}

func writeJSONBytes(w http.ResponseWriter, status int, data []byte, maximum int) {
	if len(data) > maximum {
		http.Error(w, "provider response invalid", http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

// marshalBoundedListPage keeps the protocol's fixed response-frame bound even
// when a provider returns many long object keys in one otherwise-valid page.
// LIST limits are maxima, so a shortened page resumes at its last returned key.
func marshalBoundedListPage(page ListPage) ([]byte, error) {
	encoded, err := json.Marshal(page)
	if err != nil || len(encoded) <= MaxControlFrameBytes {
		return encoded, err
	}
	if len(page.Items) == 0 {
		return nil, ErrProtocol
	}

	encodedItems := make([][]byte, len(page.Items))
	for i, item := range page.Items {
		encodedItems[i], err = json.Marshal(item)
		if err != nil {
			return nil, err
		}
	}
	const itemsPrefix = `{"items":[`
	const cursorSuffix = `],"next_cursor":`
	bestCount := 0
	itemBytes := 0
	for i, item := range page.Items {
		itemBytes += len(encodedItems[i])
		if i > 0 {
			itemBytes++ // comma between adjacent JSON items
		}
		encodedCursor, marshalErr := json.Marshal(item.Key)
		if marshalErr != nil {
			return nil, marshalErr
		}
		candidateSize := len(itemsPrefix) + itemBytes + len(cursorSuffix) + len(encodedCursor) + 1
		if candidateSize <= MaxControlFrameBytes {
			bestCount = i + 1
		}
	}
	if bestCount == 0 {
		return nil, ErrProtocol
	}
	bounded := ListPage{Items: page.Items[:bestCount], NextCursor: page.Items[bestCount-1].Key}
	return json.Marshal(bounded)
}
func writeObjectHeaders(w http.ResponseWriter, info ObjectInfo) bool {
	if info.Size < 0 || info.Size > MaxObjectBytes || !validDigest(info.SHA256) {
		return false
	}
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	w.Header().Set("X-IR-Object-SHA256", info.SHA256)
	return true
}
func oneQuery(r *http.Request, key string) (string, bool) {
	values, exists := r.URL.Query()[key]
	return firstQuery(values, exists)
}
func optionalOneQuery(r *http.Request, key string) (string, bool) {
	values, exists := r.URL.Query()[key]
	if !exists {
		return "", true
	}
	return firstQuery(values, true)
}
func firstQuery(values []string, exists bool) (string, bool) {
	return func() (string, bool) {
		if !exists || len(values) != 1 {
			return "", false
		}
		return values[0], true
	}()
}
func validDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func queryOnly(r *http.Request, allowed ...string) bool {
	if len(allowed) == 1 && allowed[0] == "" {
		allowed = nil
	}
	for name, values := range r.URL.Query() {
		known := false
		for _, candidate := range allowed {
			if candidate == name {
				known = true
				break
			}
		}
		if !known || len(values) != 1 {
			return false
		}
	}
	return true
}

func parseRange(value string) (int64, int64, error) {
	if !strings.HasPrefix(value, "bytes=") || strings.Contains(value, ",") {
		return 0, 0, ErrProtocol
	}
	parts := strings.Split(strings.TrimPrefix(value, "bytes="), "-")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, 0, ErrProtocol
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 {
		return 0, 0, ErrProtocol
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || end < start || end-start >= MaxObjectBytes {
		return 0, 0, ErrProtocol
	}
	return start, end - start + 1, nil
}

func validatePage(page ListPage, prefix, cursor string, limit int) error {
	if page.Items == nil || len(page.Items) > limit || len(page.NextCursor) > 1024 {
		return ErrProtocol
	}
	last := cursor
	for _, item := range page.Items {
		if ValidateKey(item.Key) != nil || !strings.HasPrefix(item.Key, prefix) || item.Key <= last || item.Size < 0 || item.Size > MaxObjectBytes || item.SHA256 != "" && !validDigest(item.SHA256) {
			return ErrProtocol
		}
		last = item.Key
	}
	return nil
}
