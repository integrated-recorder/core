package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
)

const (
	maxObjectJSONBytes       = 16 << 20
	maxRecordingJSONBytes    = 64 << 20
	maxArchiveEnumeration    = 5_000_000
	maxStagingCleanupEntries = 128
)

const deletionMarkerPrefix = "recording-deletions/"

// ObjectStoreArchiveBackend owns the canonical recording/archive model while
// PhysicalObjectStore owns only atomic placement of complete byte objects.
// Every key and every domain document is validated by Core before use.
type ObjectStoreArchiveBackend struct {
	root      string
	stageDir  string
	objects   PhysicalObjectStore
	telemetry *telemetry
	poolID    string
	poolName  string
	poolKind  string

	issuesMu sync.RWMutex
	issues   []RecoveryIssue
}

var _ StorageBackend = (*ObjectStoreArchiveBackend)(nil)

func (b *ObjectStoreArchiveBackend) ioTotals() (readBytes, writeBytes, errorsTotal uint64) {
	return b.telemetry.ioTotals()
}

func (b *ObjectStoreArchiveBackend) RecoveryIssues() []RecoveryIssue {
	b.issuesMu.RLock()
	defer b.issuesMu.RUnlock()
	return append([]RecoveryIssue(nil), b.issues...)
}

func (b *ObjectStoreArchiveBackend) setRecoveryIssues(issues []RecoveryIssue) {
	b.issuesMu.Lock()
	b.issues = append([]RecoveryIssue(nil), issues...)
	b.issuesMu.Unlock()
}

func (b *ObjectStoreArchiveBackend) addRecoveryIssue(issue RecoveryIssue) {
	b.issuesMu.Lock()
	b.issues = append(b.issues, issue)
	b.issuesMu.Unlock()
}

func (b *ObjectStoreArchiveBackend) HasCanonicalPayloadIssue(recordingID string) bool {
	b.issuesMu.RLock()
	defer b.issuesMu.RUnlock()
	for _, issue := range b.issues {
		if issue.ID == recordingID && strings.HasPrefix(issue.Code, "canonical_payload_") {
			return true
		}
	}
	return false
}

func (b *ObjectStoreArchiveBackend) NewRecordingDir(id string) error {
	if !recordingIDPattern.MatchString(id) {
		return errors.New("invalid recording id")
	}
	return nil
}

func (b *ObjectStoreArchiveBackend) CreateRecording(recording *domain.Recording) error {
	started := time.Now()
	if err := validateArchiveRecording(recording); err != nil {
		b.telemetry.recordError()
		return err
	}
	key := recordingObjectKey(recording.ID, "recording.json")
	if _, err := b.stat(context.Background(), key); err == nil {
		b.telemetry.recordError()
		return errors.New("recording already exists")
	} else if !isObjectNotFound(err) {
		b.telemetry.recordError()
		return errors.New("recording metadata is unavailable")
	}
	if recording.Tracks == nil {
		recording.Tracks = map[string]*domain.Track{}
	}
	if err := b.putJSON(context.Background(), key, recording, maxRecordingJSONBytes); err != nil {
		b.telemetry.recordError()
		return err
	}
	b.telemetry.recordWrite(uint64(marshalSize(recording)), time.Since(started))
	return nil
}

// CreateRecordingWithSidecar publishes a private initial sidecar before the
// complete root object. The root is the visibility marker, so a crash after
// sidecar PUT but before root PUT leaves only an ignored orphan.
func (b *ObjectStoreArchiveBackend) CreateRecordingWithSidecar(recording *domain.Recording, relativePath string, value any) error {
	if err := validateArchiveRecording(recording); err != nil || !validInitialSidecarPath(relativePath) || value == nil {
		return errors.New("invalid recording sidecar initialization")
	}
	sidecarData, err := json.MarshalIndent(value, "", "  ")
	if err != nil || int64(len(sidecarData)+1) > maxSidecarReadBytes || int64(len(sidecarData)+1) > maxObjectJSONBytes {
		return errors.New("recording sidecar initialization exceeds size limit")
	}
	rootKey := recordingObjectKey(recording.ID, "recording.json")
	if _, err := b.stat(context.Background(), rootKey); err == nil {
		return errors.New("recording already exists")
	} else if !isObjectNotFound(err) {
		return errors.New("recording metadata is unavailable")
	}
	if recording.Tracks == nil {
		recording.Tracks = map[string]*domain.Track{}
	}
	if err := b.SaveSidecar(recording.ID, relativePath, value); err != nil {
		return err
	}
	return b.CreateRecording(recording)
}

func (b *ObjectStoreArchiveBackend) SaveRecording(recording *domain.Recording) error {
	started := time.Now()
	if err := validateArchiveRecording(recording); err != nil {
		b.telemetry.recordError()
		return err
	}
	data, err := json.MarshalIndent(recording, "", "  ")
	if err != nil || len(data)+1 > maxRecordingJSONBytes {
		b.telemetry.recordError()
		return errors.New("recording metadata exceeds size limit")
	}
	data = append(data, '\n')
	if err = b.putBytes(context.Background(), recordingObjectKey(recording.ID, "recording.json"), data, maxRecordingJSONBytes); err != nil {
		b.telemetry.recordError()
		return err
	}
	b.telemetry.recordWrite(uint64(len(data)), time.Since(started))
	return nil
}

func validateArchiveRecording(recording *domain.Recording) error {
	if recording == nil || !recordingIDPattern.MatchString(recording.ID) {
		return errors.New("invalid recording")
	}
	if err := domain.ValidateMetadataTimeline(recording.MetadataTimeline); err != nil {
		return errors.New("invalid recording metadata timeline")
	}
	return nil
}

func marshalSize(value any) int {
	data, _ := json.Marshal(value)
	return len(data)
}

func (b *ObjectStoreArchiveBackend) LoadAll() ([]*domain.Recording, error) {
	b.setRecoveryIssues(nil)
	if err := b.resumeDeletions(); err != nil {
		return nil, err
	}
	deleted, err := b.deletionIDs()
	if err != nil {
		return nil, errors.New("recording deletion state is unavailable")
	}
	roots := make(map[string]struct{})
	err = b.walkPages(context.Background(), "recordings/", maxArchiveEnumeration, func(item PhysicalObjectInfo) error {
		const suffix = "/recording.json"
		if !strings.HasPrefix(item.Key, "recordings/") || !strings.HasSuffix(item.Key, suffix) {
			return nil
		}
		id := strings.TrimSuffix(strings.TrimPrefix(item.Key, "recordings/"), suffix)
		if recordingIDPattern.MatchString(id) {
			roots[id] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, errors.New("recording archive listing is unavailable")
	}
	ids := make([]string, 0, len(roots))
	for id := range roots {
		if _, hidden := deleted[id]; !hidden {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	var recordings []*domain.Recording
	for _, id := range ids {
		recording, readErr := b.loadRecordingForRecovery(context.Background(), id)
		if readErr != nil {
			code := "metadata_invalid"
			if isObjectNotFound(readErr) {
				code = "metadata_missing"
			}
			b.addRecoveryIssue(RecoveryIssue{ID: id, Code: code, Message: "recording metadata is unavailable; object data was preserved"})
			continue
		}
		changed, recErr := b.reconcileRecording(recording)
		if recErr != nil {
			b.addRecoveryIssue(RecoveryIssue{ID: id, Code: "reconciliation_incomplete", Message: "recording payload reconciliation was incomplete"})
		}
		invalidTrack := false
		for _, track := range recording.Tracks {
			if track == nil {
				b.addRecoveryIssue(RecoveryIssue{ID: id, Code: "track_invalid", Message: "recording contains an invalid track and was preserved"})
				invalidTrack = true
				continue
			}
			sortTrack(track)
		}
		if invalidTrack {
			continue
		}
		if recording.State == domain.StateRecording {
			markInterrupted(recording, time.Now().UTC())
			changed = true
		}
		if changed {
			if err := b.SaveRecording(recording); err != nil {
				b.addRecoveryIssue(RecoveryIssue{ID: id, Code: "metadata_recovery_write_failed", Message: "recovered metadata could not be durably saved"})
			}
		}
		recordings = append(recordings, recording)
	}
	return recordings, nil
}

func markInterrupted(recording *domain.Recording, now time.Time) {
	recording.State = domain.StateInterrupted
	recording.StoppedAt = &now
	if recording.LastError == "" {
		recording.LastError = "server restarted while recording was active"
	}
	for _, track := range recording.Tracks {
		if track == nil {
			continue
		}
		addPendingGap := func(epoch, sequence uint64) {
			for _, gap := range recording.Gaps {
				if gap.TrackID == track.ID && gap.SourceEpoch == epoch && sequence >= gap.FromSequence && sequence <= gap.ToSequence {
					return
				}
			}
			recording.Gaps = append(recording.Gaps, domain.Gap{TrackID: track.ID, SourceEpoch: epoch, FromSequence: sequence, ToSequence: sequence, DetectedAt: now, Reason: "server restarted before pending segment could be captured"})
		}
		for _, sequence := range track.PendingSequences {
			addPendingGap(0, sequence)
		}
		for _, pending := range track.PendingSegments {
			addPendingGap(pending.SourceEpoch, pending.Sequence)
		}
		track.PendingSequences = nil
		track.PendingSegments = nil
	}
}

func (b *ObjectStoreArchiveBackend) LoadAllReadOnly() ([]*domain.Recording, error) {
	return b.loadAllReadOnly(0, false)
}

func (b *ObjectStoreArchiveBackend) LoadAllReadOnlyLimit(max int) ([]*domain.Recording, error) {
	if max < 0 {
		return nil, ErrReadOnlyListLimit
	}
	return b.loadAllReadOnly(max, true)
}

func (b *ObjectStoreArchiveBackend) loadAllReadOnly(max int, bounded bool) ([]*domain.Recording, error) {
	deleted, err := b.deletionIDs()
	if err != nil {
		return nil, errors.New("recording deletion state is unavailable")
	}
	roots := map[string]struct{}{}
	err = b.walkPages(context.Background(), "recordings/", maxArchiveEnumeration, func(item PhysicalObjectInfo) error {
		if strings.HasPrefix(item.Key, "recordings/") && strings.HasSuffix(item.Key, "/recording.json") {
			id := strings.TrimSuffix(strings.TrimPrefix(item.Key, "recordings/"), "/recording.json")
			if recordingIDPattern.MatchString(id) {
				roots[id] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return nil, errors.New("recording archive listing is unavailable")
	}
	ids := make([]string, 0, len(roots))
	for id := range roots {
		if _, hidden := deleted[id]; !hidden {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	var recordings []*domain.Recording
	for _, id := range ids {
		recording, err := b.loadRecording(context.Background(), id)
		if err != nil {
			if isObjectNotFound(err) {
				continue
			}
			continue
		}
		for _, track := range recording.Tracks {
			if track == nil {
				return nil, errors.New("recording contains invalid track metadata")
			}
			sortTrack(track)
		}
		recordings = append(recordings, recording)
		if bounded && len(recordings) > max {
			return nil, ErrReadOnlyListLimit
		}
	}
	sort.Slice(recordings, func(i, j int) bool { return recordings[i].CreatedAt.After(recordings[j].CreatedAt) })
	return recordings, nil
}

func (b *ObjectStoreArchiveBackend) LoadRecordingReadOnly(id string) (*domain.Recording, error) {
	if !recordingIDPattern.MatchString(id) {
		return nil, errors.New("invalid recording id")
	}
	return b.loadRecording(context.Background(), id)
}

func (b *ObjectStoreArchiveBackend) loadRecording(ctx context.Context, id string) (*domain.Recording, error) {
	return b.loadRecordingWithTimelineValidation(ctx, id, true)
}

func (b *ObjectStoreArchiveBackend) loadRecordingForRecovery(ctx context.Context, id string) (*domain.Recording, error) {
	return b.loadRecordingWithTimelineValidation(ctx, id, false)
}

func (b *ObjectStoreArchiveBackend) loadRecordingWithTimelineValidation(ctx context.Context, id string, validateTimeline bool) (*domain.Recording, error) {
	if !recordingIDPattern.MatchString(id) {
		return nil, errors.New("invalid recording id")
	}
	data, _, err := b.readObject(ctx, recordingObjectKey(id, "recording.json"), maxRecordingJSONBytes)
	if err != nil {
		if isObjectNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var recording domain.Recording
	if err := json.Unmarshal(data, &recording); err != nil || recording.ID != id {
		return nil, errors.New("recording metadata is invalid")
	}
	if validateTimeline && domain.ValidateMetadataTimeline(recording.MetadataTimeline) != nil {
		return nil, errors.New("recording metadata timeline is invalid")
	}
	if recording.Tracks == nil {
		recording.Tracks = map[string]*domain.Track{}
	}
	for _, track := range recording.Tracks {
		if track == nil {
			return nil, errors.New("recording contains an invalid track")
		}
		sortTrack(track)
	}
	return &recording, nil
}

func recordingObjectKey(id, relative string) string { return "recordings/" + id + "/" + relative }

func (b *ObjectStoreArchiveBackend) SavePayload(id, relativePath string, src io.Reader, limit int64) (PayloadResult, error) {
	return b.SavePayloadContext(context.Background(), id, relativePath, src, limit)
}

func (b *ObjectStoreArchiveBackend) SavePayloadContext(ctx context.Context, id, relativePath string, src io.Reader, limit int64) (PayloadResult, error) {
	started := time.Now()
	result, err := b.saveObject(ctx, id, relativePath, src, limit, -1)
	b.recordPayloadWrite(result, started, err)
	return result, err
}

func (b *ObjectStoreArchiveBackend) SavePayloadExact(id, relativePath string, src io.Reader, limit, expectedSize int64) (PayloadResult, error) {
	return b.SavePayloadExactContext(context.Background(), id, relativePath, src, limit, expectedSize)
}

func (b *ObjectStoreArchiveBackend) SavePayloadExactContext(ctx context.Context, id, relativePath string, src io.Reader, limit, expectedSize int64) (PayloadResult, error) {
	started := time.Now()
	result, err := b.saveObject(ctx, id, relativePath, src, limit, expectedSize)
	b.recordPayloadWrite(result, started, err)
	return result, err
}

func (b *ObjectStoreArchiveBackend) recordPayloadWrite(result PayloadResult, started time.Time, err error) {
	if err != nil {
		b.telemetry.recordError()
		return
	}
	b.telemetry.recordWrite(uint64(result.Size), time.Since(started))
}

func (b *ObjectStoreArchiveBackend) saveObject(ctx context.Context, id, relative string, src io.Reader, limit, expected int64) (PayloadResult, error) {
	key, err := b.recordingKey(id, relative)
	if err != nil || src == nil {
		return PayloadResult{}, errors.New("invalid archive object")
	}
	return b.stageAndPut(ctx, key, src, limit, expected)
}

func (b *ObjectStoreArchiveBackend) stageAndPut(ctx context.Context, key string, src io.Reader, limit, expected int64) (PayloadResult, error) {
	if ValidateObjectKey(key) != nil || src == nil {
		return PayloadResult{}, errors.New("invalid archive object")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	max := limit
	if max <= 0 || max > MaxObjectBytes {
		max = MaxObjectBytes
	}
	if expected < -1 || expected > max {
		return PayloadResult{}, ErrPayloadSizeMismatch
	}
	stageDir, err := os.MkdirTemp(b.stageDir, "put-")
	if err != nil {
		return PayloadResult{}, errors.New("storage staging is unavailable")
	}
	if err := os.Chmod(stageDir, 0700); err != nil {
		_ = os.RemoveAll(stageDir)
		return PayloadResult{}, errors.New("storage staging is unavailable")
	}
	defer os.RemoveAll(stageDir)
	tmp := filepath.Join(stageDir, "object")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return PayloadResult{}, errors.New("storage staging is unavailable")
	}
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return PayloadResult{}, errors.New("storage staging is unavailable")
	}
	hash := sha256.New()
	reader := contextReader{ctx: ctx, reader: src}
	n, copyErr := io.CopyBuffer(io.MultiWriter(f, hash), io.LimitReader(reader, max+1), make([]byte, 64<<10))
	if copyErr == nil && n > max {
		copyErr = ErrIngestTooLarge
	}
	if copyErr == nil && expected >= 0 && n != expected {
		copyErr = fmt.Errorf("%w: got %d bytes; expected %d", ErrPayloadSizeMismatch, n, expected)
	}
	if copyErr == nil {
		copyErr = f.Sync()
	}
	closeErr := f.Close()
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return PayloadResult{}, copyErr
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	return b.putStaged(ctx, key, tmp, n, digest)
}

func (b *ObjectStoreArchiveBackend) putBytes(ctx context.Context, key string, data []byte, limit int64) error {
	if int64(len(data)) > limit || int64(len(data)) > MaxObjectBytes {
		return errors.New("storage object exceeds size limit")
	}
	_, err := b.stageAndPut(ctx, key, bytes.NewReader(data), limit, int64(len(data)))
	return err
}

func (b *ObjectStoreArchiveBackend) recordingKey(id, relative string) (string, error) {
	if !recordingIDPattern.MatchString(id) || !canonicalRelativePath(relative) {
		return "", errors.New("invalid relative storage path")
	}
	key := recordingObjectKey(id, relative)
	if err := ValidateObjectKey(key); err != nil {
		return "", errors.New("invalid relative storage path")
	}
	return key, nil
}

func (b *ObjectStoreArchiveBackend) putStaged(parent context.Context, key, path string, size int64, digest string) (PayloadResult, error) {
	if ValidateObjectKey(key) != nil || size < 0 || size > MaxObjectBytes || !validLowerDigest(digest) {
		return PayloadResult{}, errors.New("invalid archive object")
	}
	f, err := os.Open(path)
	if err != nil {
		return PayloadResult{}, errors.New("storage staging is unavailable")
	}
	ctx, cancel := objectContext(parent)
	info, putErr := b.objects.Put(ctx, key, f, size)
	closeErr := f.Close()
	cancel()
	if putErr == nil && closeErr != nil {
		putErr = closeErr
	}
	if putErr == nil {
		if !validPutInfo(info, key, size, digest) {
			b.telemetry.recordError()
			return PayloadResult{}, errors.New("physical object publication was not verified")
		}
		return PayloadResult{Size: size, SHA256: digest}, nil
	}
	// A transport failure may be ambiguous. Accept only a complete object whose
	// bytes independently match the staged source; otherwise leave the caller's
	// retry path in control and report a safe error.
	if confirmErr := b.confirmObject(parent, key, size, digest); confirmErr == nil {
		return PayloadResult{Size: size, SHA256: digest}, nil
	}
	b.telemetry.recordError()
	return PayloadResult{}, errors.New("physical object publication failed")
}

func (b *ObjectStoreArchiveBackend) confirmObject(parent context.Context, key string, size int64, digest string) error {
	info, err := b.stat(parent, key)
	if err != nil || info.Size != size {
		return errors.New("object outcome is unknown")
	}
	reader, opened, cancel, err := b.open(parent, key)
	if err != nil {
		return err
	}
	defer cancel()
	defer reader.Close()
	if opened.Key != key || opened.Size != size {
		return errors.New("object outcome is unknown")
	}
	h := sha256.New()
	n, err := io.Copy(h, contextReader{ctx: parentContextOrBackground(parent), reader: reader})
	if err != nil || n != size || hex.EncodeToString(h.Sum(nil)) != digest {
		return errors.New("object outcome is unknown")
	}
	return nil
}

func validPutInfo(info PhysicalObjectInfo, key string, size int64, digest string) bool {
	return ValidateObjectKey(info.Key) == nil && info.Key == key && info.Size == size && info.SHA256 == digest && validLowerDigest(info.SHA256)
}

func validLowerDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func objectContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parentContextOrBackground(parent), objectCallTimeout)
}

func parentContextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func isObjectNotFound(err error) bool {
	return errors.Is(err, ErrObjectNotFound) || errors.Is(err, os.ErrNotExist)
}

func (b *ObjectStoreArchiveBackend) stat(parent context.Context, key string) (PhysicalObjectInfo, error) {
	if ValidateObjectKey(key) != nil {
		return PhysicalObjectInfo{}, errors.New("invalid archive object")
	}
	ctx, cancel := objectContext(parent)
	defer cancel()
	info, err := b.objects.Stat(ctx, key)
	if err != nil {
		return PhysicalObjectInfo{}, err
	}
	if info.Key != key || info.Size < 0 || info.Size > MaxObjectBytes || info.SHA256 != "" && !validLowerDigest(info.SHA256) {
		return PhysicalObjectInfo{}, errors.New("physical object metadata is invalid")
	}
	return info, nil
}

func (b *ObjectStoreArchiveBackend) open(parent context.Context, key string) (io.ReadCloser, PhysicalObjectInfo, context.CancelFunc, error) {
	if ValidateObjectKey(key) != nil {
		return nil, PhysicalObjectInfo{}, func() {}, errors.New("invalid archive object")
	}
	ctx, cancel := objectContext(parent)
	reader, info, err := b.objects.Open(ctx, key)
	if err != nil {
		cancel()
		return nil, PhysicalObjectInfo{}, func() {}, err
	}
	if reader == nil || info.Key != key || info.Size < 0 || info.Size > MaxObjectBytes || info.SHA256 != "" && !validLowerDigest(info.SHA256) {
		if reader != nil {
			_ = reader.Close()
		}
		cancel()
		return nil, PhysicalObjectInfo{}, func() {}, errors.New("physical object metadata is invalid")
	}
	return reader, info, cancel, nil
}

type cancelReadCloser struct {
	reader io.ReadCloser
	cancel context.CancelFunc
	once   sync.Once
	err    error
}

func (r *cancelReadCloser) Read(p []byte) (int, error) { return r.reader.Read(p) }
func (r *cancelReadCloser) Close() error {
	r.once.Do(func() {
		r.err = r.reader.Close()
		r.cancel()
	})
	return r.err
}

func (b *ObjectStoreArchiveBackend) OpenPayloadReader(id, relativePath string) (io.ReadCloser, error) {
	key, err := b.recordingKey(id, relativePath)
	if err != nil {
		return nil, err
	}
	reader, _, cancel, err := b.open(context.Background(), key)
	if err != nil {
		b.telemetry.recordError()
		return nil, err
	}
	return &cancelReadCloser{reader: &meteredObjectReadCloser{reader: reader, telemetry: b.telemetry}, cancel: cancel}, nil
}

type meteredObjectReadCloser struct {
	reader    io.ReadCloser
	telemetry *telemetry
}

func (r *meteredObjectReadCloser) Read(p []byte) (int, error) {
	started := time.Now()
	n, err := r.reader.Read(p)
	if n > 0 {
		r.telemetry.recordRead(uint64(n), 1, time.Since(started))
	}
	if err != nil && !errors.Is(err, io.EOF) {
		r.telemetry.recordError()
	}
	return n, err
}
func (r *meteredObjectReadCloser) Close() error { return r.reader.Close() }

func (b *ObjectStoreArchiveBackend) OpenPayloadRange(id, relativePath string, offset, length int64) (io.ReadCloser, ObjectInfo, error) {
	reader, info, err := b.OpenPayloadRangeContext(context.Background(), id, relativePath, offset, length)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	return reader, ObjectInfo{Size: length, ModifiedAt: info.ModifiedAt, Regular: true}, nil
}

// OpenPayloadRangeContext opens only the requested body range. The returned
// physical metadata retains the complete object's size, which lets an HTTP
// Range caller form Content-Range without fetching the full object.
func (b *ObjectStoreArchiveBackend) OpenPayloadRangeContext(ctx context.Context, id, relativePath string, offset, length int64) (io.ReadCloser, PhysicalObjectInfo, error) {
	key, err := b.recordingKey(id, relativePath)
	if err != nil || offset < 0 || length <= 0 || length > MaxObjectBytes || offset > MaxObjectBytes-length {
		return nil, PhysicalObjectInfo{}, errors.New("invalid archive range")
	}
	callCtx, cancel := objectContext(ctx)
	reader, info, err := b.objects.OpenRange(callCtx, key, offset, length)
	if err != nil {
		cancel()
		b.telemetry.recordError()
		return nil, PhysicalObjectInfo{}, errors.New("archive object range is unavailable")
	}
	if reader == nil || info.Key != key || info.Size < length || offset > info.Size-length {
		if reader != nil {
			_ = reader.Close()
		}
		cancel()
		return nil, PhysicalObjectInfo{}, errors.New("physical object range metadata is invalid")
	}
	return &cancelReadCloser{reader: &meteredObjectReadCloser{reader: reader, telemetry: b.telemetry}, cancel: cancel}, info, nil
}

// OpenPayloadRangeReaderContext is the backend-neutral range reader exposed to
// Core callers. The physical range API retains the full-object metadata for
// HTTP Content-Range construction; callers of this method need only the
// bounded stream.
func (b *ObjectStoreArchiveBackend) OpenPayloadRangeReaderContext(ctx context.Context, id, relativePath string, offset, length int64) (io.ReadCloser, error) {
	reader, _, err := b.OpenPayloadRangeContext(ctx, id, relativePath, offset, length)
	return reader, err
}

func (b *ObjectStoreArchiveBackend) StatPayload(id, relativePath string) (ObjectInfo, error) {
	key, err := b.recordingKey(id, relativePath)
	if err != nil {
		return ObjectInfo{}, err
	}
	info, err := b.stat(context.Background(), key)
	if err != nil {
		b.telemetry.recordError()
		return ObjectInfo{}, err
	}
	return ObjectInfo{Size: info.Size, ModifiedAt: info.ModifiedAt, Regular: true}, nil
}

func (b *ObjectStoreArchiveBackend) OpenPayload(id, relativePath string) (*os.File, error) {
	return nil, errors.New("concrete file access is unavailable for this storage backend")
}

func (b *ObjectStoreArchiveBackend) SaveSidecar(id, relativePath string, value any) error {
	if _, err := b.recordingKey(id, relativePath); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil || len(data)+1 > maxObjectJSONBytes {
		return errors.New("sidecar exceeds size limit")
	}
	data = append(data, '\n')
	started := time.Now()
	if err := b.putBytes(context.Background(), recordingObjectKey(id, relativePath)+".json", data, maxObjectJSONBytes); err != nil {
		b.telemetry.recordError()
		return err
	}
	b.telemetry.recordWrite(uint64(len(data)), time.Since(started))
	return nil
}

// LoadSidecar is an optional, read-only JSON sidecar capability. The physical
// provider returns bounded bytes; Core alone decodes the archive document.
func (b *ObjectStoreArchiveBackend) LoadSidecar(id, relativePath string, maxBytes int64, output any) error {
	if err := validateSidecarRead(id, relativePath, maxBytes, output); err != nil {
		return err
	}
	key, err := b.recordingKey(id, relativePath)
	if err != nil {
		return err
	}
	key += ".json"
	if err := ValidateObjectKey(key); err != nil {
		return errors.New("invalid sidecar reference")
	}
	started := time.Now()
	data, _, err := b.readObject(context.Background(), key, maxBytes)
	if err != nil {
		b.telemetry.recordError()
		return err
	}
	if err := decodeStrictSidecar(data, output); err != nil {
		b.telemetry.recordError()
		return err
	}
	b.telemetry.recordRead(uint64(len(data)), 1, time.Since(started))
	return nil
}

func (b *ObjectStoreArchiveBackend) SaveSnapshot(id, trackID, sourceURI string, data []byte, at time.Time) (domain.ManifestSnapshot, error) {
	if len(data) > 4<<20 {
		return domain.ManifestSnapshot{}, errors.New("manifest exceeds size limit")
	}
	digest := sha256.Sum256(data)
	hexDigest := hex.EncodeToString(digest[:])
	name := fmt.Sprintf("manifests/%s-%d-%s.m3u8", safeName(trackID), at.UnixNano(), hexDigest[:12])
	if _, err := b.SavePayload(id, name, bytes.NewReader(data), 4<<20); err != nil {
		return domain.ManifestSnapshot{}, err
	}
	snapshot := domain.ManifestSnapshot{TrackID: trackID, SourceURI: sourceURI, StoragePath: name, FetchedAt: at, SHA256: hexDigest, Size: int64(len(data))}
	if err := b.SaveSidecar(id, name, snapshot); err != nil {
		return domain.ManifestSnapshot{}, err
	}
	return snapshot, nil
}

func (b *ObjectStoreArchiveBackend) listPage(parent context.Context, prefix, cursor string, limit int) (PhysicalObjectPage, error) {
	if validateObjectPrefix(prefix) != nil || len(cursor) > 1024 || limit < 1 || limit > maxObjectPageSize {
		return PhysicalObjectPage{}, errors.New("invalid archive listing request")
	}
	ctx, cancel := objectContext(parent)
	defer cancel()
	page, err := b.objects.List(ctx, prefix, cursor, limit)
	if err != nil {
		return PhysicalObjectPage{}, err
	}
	if len(page.Items) > limit || len(page.NextCursor) > 1024 {
		return PhysicalObjectPage{}, errors.New("physical object listing is invalid")
	}
	previous := cursor
	for _, item := range page.Items {
		if ValidateObjectKey(item.Key) != nil || !strings.HasPrefix(item.Key, prefix) || item.Key <= previous || item.Size < 0 || item.Size > MaxObjectBytes || item.SHA256 != "" && !validLowerDigest(item.SHA256) {
			return PhysicalObjectPage{}, errors.New("physical object listing is invalid")
		}
		previous = item.Key
	}
	if page.NextCursor != "" && (len(page.Items) == 0 || page.NextCursor != previous) {
		return PhysicalObjectPage{}, errors.New("physical object cursor is invalid")
	}
	return page, nil
}

func (b *ObjectStoreArchiveBackend) listAll(parent context.Context, prefix string, maximum int) ([]PhysicalObjectInfo, error) {
	if maximum < 0 {
		return nil, errors.New("invalid archive listing limit")
	}
	items := make([]PhysicalObjectInfo, 0)
	err := b.walkPages(parent, prefix, maximum, func(item PhysicalObjectInfo) error {
		items = append(items, item)
		return nil
	})
	return items, err
}

func (b *ObjectStoreArchiveBackend) walkPages(parent context.Context, prefix string, maximum int, visit func(PhysicalObjectInfo) error) error {
	if maximum < 0 || visit == nil {
		return errors.New("invalid archive listing limit")
	}
	cursor := ""
	seen := 0
	for pages := 0; pages <= maximum/maxObjectPageSize+1; pages++ {
		page, err := b.listPage(parent, prefix, cursor, maxObjectPageSize)
		if err != nil {
			return err
		}
		if seen+len(page.Items) > maximum {
			return errors.New("archive object listing exceeds limit")
		}
		for _, item := range page.Items {
			if err := visit(item); err != nil {
				return err
			}
			seen++
		}
		if page.NextCursor == "" {
			return nil
		}
		if page.NextCursor == cursor {
			return errors.New("physical object cursor did not advance")
		}
		cursor = page.NextCursor
	}
	return errors.New("archive object listing exceeds page limit")
}

func (b *ObjectStoreArchiveBackend) readObject(ctx context.Context, key string, limit int64) ([]byte, PhysicalObjectInfo, error) {
	info, err := b.stat(ctx, key)
	if err != nil {
		return nil, PhysicalObjectInfo{}, err
	}
	if limit <= 0 || limit > MaxObjectBytes || info.Size > limit {
		return nil, PhysicalObjectInfo{}, errors.New("archive document exceeds size limit")
	}
	reader, opened, cancel, err := b.open(ctx, key)
	if err != nil {
		return nil, PhysicalObjectInfo{}, err
	}
	defer cancel()
	defer reader.Close()
	if opened.Size != info.Size {
		return nil, PhysicalObjectInfo{}, errors.New("archive object changed during read")
	}
	data, err := io.ReadAll(io.LimitReader(contextReader{ctx: parentContextOrBackground(ctx), reader: reader}, limit+1))
	if err != nil || int64(len(data)) != info.Size || int64(len(data)) > limit {
		return nil, PhysicalObjectInfo{}, errors.New("archive object read is incomplete")
	}
	return data, info, nil
}

func (b *ObjectStoreArchiveBackend) putJSON(ctx context.Context, key string, value any, limit int64) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil || int64(len(data))+1 > limit {
		return errors.New("archive document exceeds size limit")
	}
	data = append(data, '\n')
	return b.putBytes(ctx, key, data, limit)
}

func (b *ObjectStoreArchiveBackend) deletionMarkerKey(id string) string {
	return deletionMarkerPrefix + id
}

func (b *ObjectStoreArchiveBackend) deletionIDs() (map[string]struct{}, error) {
	items, err := b.listAll(context.Background(), deletionMarkerPrefix, maxArchiveEnumeration)
	if err != nil {
		return nil, err
	}
	deleted := make(map[string]struct{}, len(items))
	for _, item := range items {
		id := strings.TrimPrefix(item.Key, deletionMarkerPrefix)
		if recordingIDPattern.MatchString(id) && !strings.Contains(id, "/") {
			deleted[id] = struct{}{}
		}
	}
	return deleted, nil
}

func (b *ObjectStoreArchiveBackend) resumeDeletions() error {
	items, err := b.listAll(context.Background(), deletionMarkerPrefix, maxArchiveEnumeration)
	if err != nil {
		return err
	}
	for _, item := range items {
		id := strings.TrimPrefix(item.Key, deletionMarkerPrefix)
		if !recordingIDPattern.MatchString(id) || strings.Contains(id, "/") {
			continue
		}
		if err := b.deleteRecordingObjects(id); err != nil {
			b.addRecoveryIssue(RecoveryIssue{ID: id, Code: "deletion_cleanup_failed", Message: "a pending recording deletion could not be completed"})
			continue
		}
		if err := b.delete(context.Background(), item.Key); err != nil {
			b.addRecoveryIssue(RecoveryIssue{ID: id, Code: "deletion_cleanup_failed", Message: "a pending recording deletion could not be completed"})
		}
	}
	return nil
}

// DeleteRecordingData first publishes a logical tombstone. LoadAll ignores a
// tombstoned archive and startup recovery retries bounded object deletion.
func (b *ObjectStoreArchiveBackend) DeleteRecordingData(id string) error {
	if !recordingIDPattern.MatchString(id) {
		return errors.New("invalid recording id")
	}
	marker := b.deletionMarkerKey(id)
	if err := b.putBytes(context.Background(), marker, []byte(id), 64); err != nil {
		b.telemetry.recordError()
		return errors.New("recording deletion could not be prepared")
	}
	if err := b.deleteRecordingObjects(id); err != nil {
		b.telemetry.recordError()
		return errors.New("recording deletion could not be completed")
	}
	if err := b.delete(context.Background(), marker); err != nil {
		b.telemetry.recordError()
		return errors.New("recording deletion could not be completed")
	}
	return nil
}

func (b *ObjectStoreArchiveBackend) deleteRecordingObjects(id string) error {
	prefix := "recordings/" + id + "/"
	items, err := b.listAll(context.Background(), prefix, maxObjectEnumeration)
	if err != nil {
		return err
	}
	for _, item := range items {
		if !strings.HasPrefix(item.Key, prefix) || ValidateObjectKey(item.Key) != nil {
			return errors.New("recording object listing is invalid")
		}
		if err := b.delete(context.Background(), item.Key); err != nil {
			return err
		}
	}
	remaining, err := b.listPage(context.Background(), prefix, "", 1)
	if err != nil || len(remaining.Items) != 0 {
		return errors.New("recording objects remain after deletion")
	}
	return nil
}

func (b *ObjectStoreArchiveBackend) delete(ctx context.Context, key string) error {
	if ValidateObjectKey(key) != nil {
		return errors.New("invalid archive object")
	}
	callCtx, cancel := objectContext(ctx)
	defer cancel()
	if err := b.objects.Delete(callCtx, key); err != nil && !isObjectNotFound(err) {
		return err
	}
	return nil
}

func (b *ObjectStoreArchiveBackend) PoolMetrics() PoolSnapshot {
	return b.poolMetrics(IngestSnapshot{})
}

func (b *ObjectStoreArchiveBackend) poolMetrics(ingest IngestSnapshot) PoolSnapshot {
	return b.poolMetricsAt(ingest, time.Now())
}

func (b *ObjectStoreArchiveBackend) samplePool(ingest IngestSnapshot, now time.Time) {
	b.telemetry.sample(now, ingest)
}

func (b *ObjectStoreArchiveBackend) poolMetricsAt(ingest IngestSnapshot, now time.Time) PoolSnapshot {
	throughput, errorsTotal, samples := b.telemetry.sample(now, ingest)
	health := "healthy"
	if b.telemetry.degraded() {
		health = "degraded"
	}
	id, name, kind := b.poolID, b.poolName, b.poolKind
	if id == "" {
		id, name, kind = "remote-primary", "기본 보관 저장소", "remote"
	}
	return PoolSnapshot{ID: id, DisplayName: name, Kind: kind, Role: "primary", Health: health, CapacityKnown: false, Throughput: throughput, EstimatedCeiling: estimateCeiling(samples), ErrorsTotal: errorsTotal, Samples: samples}
}

func (b *ObjectStoreArchiveBackend) ArchiveIndex(recording *domain.Recording) ([]ArchiveEntry, error) {
	if recording == nil || !recordingIDPattern.MatchString(recording.ID) {
		return nil, errors.New("invalid recording")
	}
	rootInfo, err := b.stat(context.Background(), recordingObjectKey(recording.ID, "recording.json"))
	if err != nil {
		return nil, errors.New("recording metadata is unavailable")
	}
	entries := []ArchiveEntry{{Path: "recording.json", Kind: "recording", Size: rootInfo.Size}}
	seen := map[string]struct{}{"recording.json": {}}
	add := func(path, kind, prefix, digest string) error {
		if !archiveReferencePath(path, prefix) {
			return errors.New("invalid canonical archive reference")
		}
		if _, exists := seen[path]; exists {
			return errors.New("duplicate canonical archive reference")
		}
		info, statErr := b.stat(context.Background(), recordingObjectKey(recording.ID, path))
		if statErr != nil {
			return errors.New("canonical archive payload is unavailable")
		}
		seen[path] = struct{}{}
		entries = append(entries, ArchiveEntry{Path: path, Kind: kind, Size: info.Size, SHA256: digest})
		sidecar := path + ".json"
		if _, exists := seen[sidecar]; exists {
			return errors.New("duplicate canonical archive reference")
		}
		sideInfo, sideErr := b.stat(context.Background(), recordingObjectKey(recording.ID, sidecar))
		if isObjectNotFound(sideErr) {
			return nil
		}
		if sideErr != nil {
			return errors.New("canonical archive sidecar is unavailable")
		}
		seen[sidecar] = struct{}{}
		entries = append(entries, ArchiveEntry{Path: sidecar, Kind: kind + "_sidecar", Size: sideInfo.Size})
		return nil
	}
	for _, snapshot := range recording.Snapshots {
		if err := add(snapshot.StoragePath, "manifest", "manifests/", snapshot.SHA256); err != nil {
			return nil, err
		}
	}
	for _, track := range recording.Tracks {
		if track == nil {
			return nil, errors.New("recording contains invalid track metadata")
		}
		for _, segment := range track.Segments {
			if err := add(segment.StoragePath, "segment", "tracks/", segment.SHA256); err != nil {
				return nil, err
			}
		}
		for _, segment := range track.InitSegments {
			if err := add(segment.StoragePath, "init_segment", "tracks/", segment.SHA256); err != nil {
				return nil, err
			}
		}
	}
	sortArchiveEntries(entries)
	return entries, nil
}

func (b *ObjectStoreArchiveBackend) RecordingDirectoryBytes(id string) (int64, error) {
	return b.RecordingDirectoryBytesContext(context.Background(), id)
}

// RecordingDirectoryBytesContext returns physical bytes under one recording
// using the caller's cancellation context for provider listing.
func (b *ObjectStoreArchiveBackend) RecordingDirectoryBytesContext(ctx context.Context, id string) (int64, error) {
	if !recordingIDPattern.MatchString(id) {
		return 0, errors.New("invalid recording id")
	}
	items, err := b.listAll(ctx, "recordings/"+id+"/", maxObjectEnumeration)
	if err != nil {
		return 0, errors.New("recording archive listing is unavailable")
	}
	var total int64
	for _, item := range items {
		if item.Size < 0 {
			return 0, errors.New("recording archive contains an invalid object size")
		}
		if total > math.MaxInt64-item.Size {
			return 0, ErrArchiveSizeOverflow
		}
		total += item.Size
	}
	return total, nil
}

func (b *ObjectStoreArchiveBackend) VerifyRecording(recording *domain.Recording) IntegrityResult {
	return b.VerifyRecordingContext(context.Background(), recording)
}

func (b *ObjectStoreArchiveBackend) VerifyRecordingContext(ctx context.Context, recording *domain.Recording) IntegrityResult {
	if ctx == nil {
		ctx = context.Background()
	}
	result := IntegrityResult{Status: IntegrityVerified, LastVerifiedAt: time.Now().UTC(), Issues: []IntegrityIssue{}}
	if recording == nil || !recordingIDPattern.MatchString(recording.ID) {
		result.Status = IntegrityFailed
		result.Issues = append(result.Issues, IntegrityIssue{Code: "invalid_recording"})
		return result
	}
	canceled := false
	add := func(path, expectedPrefix string, expectedSize int64, expectedHash string) {
		if ctx.Err() != nil {
			canceled = true
			return
		}
		result.ObjectsTotal++
		if !archiveReferencePath(path, expectedPrefix) || expectedSize < 0 {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, IntegrityIssue{Code: "invalid_reference"})
			return
		}
		digest, err := hex.DecodeString(expectedHash)
		if err != nil || len(digest) != sha256.Size {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, IntegrityIssue{Code: "invalid_integrity_metadata", Path: path})
			return
		}
		key := recordingObjectKey(recording.ID, path)
		info, err := b.stat(ctx, key)
		if isObjectNotFound(err) {
			result.ObjectsMissing++
			result.Issues = append(result.Issues, IntegrityIssue{Code: "missing_payload", Path: path})
			return
		}
		if err != nil || info.Size != expectedSize {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, IntegrityIssue{Code: "unavailable_payload", Path: path})
			return
		}
		reader, opened, cancel, err := b.open(ctx, key)
		if err != nil {
			if isObjectNotFound(err) {
				result.ObjectsMissing++
				result.Issues = append(result.Issues, IntegrityIssue{Code: "missing_payload", Path: path})
			} else {
				result.ObjectsCorrupt++
				result.Issues = append(result.Issues, IntegrityIssue{Code: "unavailable_payload", Path: path})
			}
			return
		}
		h := sha256.New()
		size, copyErr := io.Copy(h, contextReader{ctx: ctx, reader: reader})
		closeErr := reader.Close()
		cancel()
		if ctx.Err() != nil || errors.Is(copyErr, context.Canceled) || errors.Is(copyErr, context.DeadlineExceeded) {
			canceled = true
			return
		}
		if opened.Size != expectedSize || copyErr != nil || closeErr != nil || size != expectedSize || !bytes.Equal(h.Sum(nil), digest) {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, IntegrityIssue{Code: "payload_mismatch", Path: path})
			return
		}
		result.ObjectsVerified++
	}
	for _, snapshot := range recording.Snapshots {
		if canceled {
			break
		}
		add(snapshot.StoragePath, "manifests/", snapshot.Size, snapshot.SHA256)
	}
	for _, track := range recording.Tracks {
		if track == nil {
			result.ObjectsCorrupt++
			result.Issues = append(result.Issues, IntegrityIssue{Code: "invalid_track"})
			continue
		}
		for _, segment := range track.Segments {
			if canceled {
				break
			}
			add(segment.StoragePath, "tracks/", segment.PayloadSize, segment.SHA256)
		}
		for _, segment := range track.InitSegments {
			if canceled {
				break
			}
			add(segment.StoragePath, "tracks/", segment.PayloadSize, segment.SHA256)
		}
	}
	if len(result.Issues) > 0 {
		result.Status = IntegrityDegraded
	}
	if canceled {
		return IntegrityResult{Status: IntegrityUnknown, Issues: []IntegrityIssue{}}
	}
	return result
}

func (b *ObjectStoreArchiveBackend) StorageStats() (StorageStats, error) {
	stats := StorageStats{ArchiveRoot: b.root, CapacityKnown: false}
	deleted, err := b.deletionIDs()
	if err != nil {
		return StorageStats{}, errors.New("recording deletion state is unavailable")
	}
	roots := map[string]struct{}{}
	err = b.walkPages(context.Background(), "recordings/", maxArchiveEnumeration, func(item PhysicalObjectInfo) error {
		id := ""
		if strings.HasPrefix(item.Key, "recordings/") {
			parts := strings.SplitN(strings.TrimPrefix(item.Key, "recordings/"), "/", 2)
			if len(parts) == 2 {
				id = parts[0]
			}
		}
		if _, hidden := deleted[id]; hidden {
			return nil
		}
		if strings.HasPrefix(item.Key, "recordings/") && strings.HasSuffix(item.Key, "/recording.json") {
			id := strings.TrimSuffix(strings.TrimPrefix(item.Key, "recordings/"), "/recording.json")
			if recordingIDPattern.MatchString(id) {
				roots[id] = struct{}{}
			}
		}
		if item.Size < 0 || uint64(item.Size) > math.MaxUint64-stats.RecordingBytes {
			return errors.New("recording storage size overflow")
		}
		stats.RecordingBytes += uint64(item.Size)
		return nil
	})
	if err != nil {
		return StorageStats{}, errors.New("recording archive listing is unavailable")
	}
	for id := range roots {
		recording, err := b.loadRecordingForRecovery(context.Background(), id)
		if err != nil {
			return StorageStats{}, errors.New("canonical recording metadata unavailable")
		}
		stats.RecordingCount++
		stats.ManifestCount += len(recording.Snapshots)
		for _, track := range recording.Tracks {
			if track == nil {
				return StorageStats{}, errors.New("canonical recording metadata invalid")
			}
			stats.SegmentCount += len(track.Segments)
			stats.InitSegmentCount += len(track.InitSegments)
		}
	}
	return stats, nil
}

func (b *ObjectStoreArchiveBackend) reconcileRecording(recording *domain.Recording) (bool, error) {
	prefix := "recordings/" + recording.ID + "/"
	items, err := b.listAll(context.Background(), prefix, maxObjectEnumeration)
	if err != nil {
		return false, err
	}
	known := make(map[string]struct{})
	for _, snapshot := range recording.Snapshots {
		known[snapshot.StoragePath] = struct{}{}
	}
	for _, track := range recording.Tracks {
		if track == nil {
			continue
		}
		for _, segment := range track.Segments {
			known[segment.StoragePath] = struct{}{}
		}
		for _, segment := range track.InitSegments {
			known[segment.StoragePath] = struct{}{}
		}
	}
	changed := false
	for _, item := range items {
		if strings.HasSuffix(item.Key, "/recording.json") || !strings.HasSuffix(item.Key, ".json") {
			continue
		}
		relative := strings.TrimPrefix(item.Key, prefix)
		if !canonicalRelativePath(relative) || len(relative) <= len(".json") {
			continue
		}
		payloadPath := strings.TrimSuffix(relative, ".json")
		data, _, readErr := b.readObject(context.Background(), item.Key, maxObjectJSONBytes)
		if readErr != nil {
			b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: "sidecar_unavailable", Message: "segment metadata sidecar could not be read"})
			continue
		}
		if strings.HasPrefix(payloadPath, "manifests/") {
			var snapshot domain.ManifestSnapshot
			if json.Unmarshal(data, &snapshot) != nil || snapshot.TrackID == "" || snapshot.StoragePath != payloadPath || snapshot.Size < 0 || !validLowerDigest(snapshot.SHA256) {
				b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: "manifest_sidecar_invalid", Message: "manifest metadata sidecar is invalid"})
				continue
			}
			if _, ok := recording.Tracks[snapshot.TrackID]; !ok {
				b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: "manifest_track_unknown", Message: "manifest metadata refers to an undeclared track"})
				continue
			}
			if err := b.verifyCanonicalObject(recording.ID, payloadPath, snapshot.Size, snapshot.SHA256); err != nil {
				code, message := "manifest_payload_mismatch", "manifest payload failed size or integrity verification"
				if isObjectNotFound(err) {
					code, message = "manifest_payload_unavailable", "manifest payload for committed metadata is unavailable"
				}
				b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: code, Message: message})
				continue
			}
			found := false
			for _, existing := range recording.Snapshots {
				if existing.StoragePath == payloadPath {
					found = true
					if existing.SHA256 != snapshot.SHA256 || existing.Size != snapshot.Size || existing.TrackID != snapshot.TrackID {
						b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: "manifest_sidecar_conflict", Message: "manifest metadata conflicts with the canonical recording document"})
					}
					break
				}
			}
			if found {
				continue
			}
			if _, conflict := known[payloadPath]; conflict {
				b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: "manifest_sidecar_conflict", Message: "manifest storage path is already assigned in the canonical recording document"})
				continue
			}
			recording.Snapshots = append(recording.Snapshots, snapshot)
			known[payloadPath] = struct{}{}
			changed = true
			continue
		}
		if !strings.HasPrefix(payloadPath, "tracks/") {
			continue
		}
		var segment domain.Segment
		if json.Unmarshal(data, &segment) != nil || segment.ID == "" || segment.TrackID == "" || segment.StoragePath != payloadPath || segment.PayloadSize < 0 || !validLowerDigest(segment.SHA256) {
			b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: "sidecar_invalid", Message: "segment metadata sidecar is invalid"})
			continue
		}
		track := recording.Tracks[segment.TrackID]
		if track == nil {
			b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: "sidecar_track_unknown", Message: "segment metadata refers to an undeclared track"})
			continue
		}
		if err := b.verifyCanonicalObject(recording.ID, payloadPath, segment.PayloadSize, segment.SHA256); err != nil {
			code, message := "sidecar_payload_mismatch", "segment payload failed size or integrity verification"
			if isObjectNotFound(err) {
				code, message = "sidecar_payload_unavailable", "segment payload for committed metadata is unavailable"
			}
			b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: code, Message: message})
			continue
		}
		if existing, duplicate := findSegment(track, segment.ID, payloadPath); duplicate {
			if existing.StoragePath != segment.StoragePath || existing.SHA256 != segment.SHA256 || existing.PayloadSize != segment.PayloadSize || existing.IsInit != segment.IsInit {
				b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: "sidecar_conflict", Message: "segment metadata conflicts with the canonical recording document"})
			}
			continue
		}
		if _, conflict := known[payloadPath]; conflict {
			b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: "sidecar_conflict", Message: "segment storage path is already assigned in the canonical recording document"})
			continue
		}
		if segment.IsInit {
			track.InitSegments = append(track.InitSegments, segment)
		} else {
			track.Segments = append(track.Segments, segment)
		}
		known[payloadPath] = struct{}{}
		changed = true
	}
	if err := domain.ValidateMetadataTimeline(recording.MetadataTimeline); err != nil {
		recording.MetadataTimeline = nil
		recording.MetadataTimelineTruncated = true
		changed = true
		b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: "metadata_timeline_invalid", Message: "source metadata timeline was invalid and could not be restored"})
	}
	for _, snapshot := range recording.Snapshots {
		if err := b.verifyCanonicalObject(recording.ID, snapshot.StoragePath, snapshot.Size, snapshot.SHA256); err != nil {
			code, message := "manifest_payload_mismatch", "manifest payload failed size or integrity verification"
			if isObjectNotFound(err) {
				code, message = "manifest_payload_unavailable", "manifest payload for committed metadata is unavailable"
			}
			b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: code, Message: message})
		}
	}
	for _, track := range recording.Tracks {
		if track == nil {
			continue
		}
		for _, segment := range append(append([]domain.Segment(nil), track.Segments...), track.InitSegments...) {
			if err := b.verifyCanonicalObject(recording.ID, segment.StoragePath, segment.PayloadSize, segment.SHA256); err != nil {
				code := "canonical_payload_mismatch"
				if isObjectNotFound(err) {
					code = "canonical_payload_unavailable"
				}
				b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: code, Message: "canonical payload failed size or integrity verification; metadata was preserved"})
			}
		}
	}
	for _, item := range items {
		relative := strings.TrimPrefix(item.Key, prefix)
		if strings.HasPrefix(relative, "tracks/") && !strings.HasSuffix(relative, ".json") {
			if _, exists := known[relative]; !exists {
				if _, sideErr := b.stat(context.Background(), item.Key+".json"); isObjectNotFound(sideErr) {
					b.addRecoveryIssue(RecoveryIssue{ID: recording.ID, Code: "orphan_payload", Message: "track payload without committed metadata was preserved"})
				}
			}
		}
	}
	return changed, nil
}

func (b *ObjectStoreArchiveBackend) verifyCanonicalObject(id, relative string, size int64, digest string) error {
	if !canonicalRelativePath(relative) || size < 0 || !validLowerDigest(digest) {
		return errors.New("invalid canonical reference")
	}
	key := recordingObjectKey(id, relative)
	info, err := b.stat(context.Background(), key)
	if isObjectNotFound(err) {
		return ErrObjectNotFound
	}
	if err != nil || info.Size != size {
		return errors.New("canonical object unavailable")
	}
	reader, opened, cancel, err := b.open(context.Background(), key)
	if err != nil {
		return err
	}
	defer cancel()
	defer reader.Close()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(reader, MaxObjectBytes+1))
	if err != nil || opened.Size != size || n != size || hex.EncodeToString(h.Sum(nil)) != digest {
		return errors.New("canonical object mismatch")
	}
	return nil
}

func (b *ObjectStoreArchiveBackend) readObjectIntoJSON(ctx context.Context, key string, maximum int64, output any) error {
	data, _, err := b.readObject(ctx, key, maximum)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, output); err != nil {
		return errors.New("archive document is invalid")
	}
	return nil
}

func cleanupObjectStaging(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	remaining := maxStagingCleanupEntries
	for _, entry := range entries {
		if remaining <= 0 {
			break
		}
		remaining--
		if !strings.HasPrefix(entry.Name(), ".object-stage-") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		info, err := os.Lstat(path)
		if err == nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
			_ = os.Remove(path)
		}
	}
}

func ensurePrivateStorageDirectory(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe storage directory")
	}
	return os.Chmod(path, 0700)
}

func ensureStorageRoot(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe storage root")
	}
	return nil
}
