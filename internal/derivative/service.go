// Package derivative builds optional user-facing projections from canonical
// recordings. It never modifies recording archives.
package derivative

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	pathpkg "path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/integrated-recorder/core/internal/domain"
	"github.com/integrated-recorder/core/internal/storage"
)

const (
	maxWorkers       = 4
	maxActiveJobs    = 64
	maxHistory       = 5000
	maxInputBytes    = int64(512 << 30)
	defaultTimeout   = 30 * time.Minute
	jobKindExport    = "export"
	defaultFFmpegBin = "ffmpeg"
)

var (
	jobIDPattern       = regexp.MustCompile(`^[a-f0-9]{32}$`)
	recordingIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

	ErrUnavailable = errors.New("export backend unavailable")
	ErrUnsupported = errors.New("export format unsupported")
	ErrActive      = errors.New("recording is active")
	ErrNotFound    = errors.New("export job not found")
	ErrCapacity    = errors.New("export job capacity reached")
	ErrInvalid     = errors.New("invalid export request")
	ErrConflict    = errors.New("export job is active")
)

// State is the durable lifecycle of one derived export.
type State string

const (
	StateQueued    State = "queued"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateCanceled  State = "canceled"
)

// Job is safe to return from the management API. It contains no paths,
// commands, source URIs, adapter state, or process output.
type Job struct {
	ID          string     `json:"id"`
	RecordingID string     `json:"recording_id"`
	Kind        string     `json:"kind"`
	State       State      `json:"state"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	OutputName  string     `json:"output_name,omitempty"`
	Size        int64      `json:"size,omitempty"`
	ErrorCode   string     `json:"error_code,omitempty"`
}

type persistedState struct {
	Version int   `json:"version"`
	Jobs    []Job `json:"jobs"`
}

type queuedJob struct {
	id        string
	recording *domain.Recording
}

type Service struct {
	mu           sync.Mutex
	root         string
	jobsDir      string
	thumbnailDir string
	statePath    string
	store        *storage.Store
	ffmpegPath   string
	timeout      time.Duration
	queue        chan queuedJob
	ctx          context.Context
	cancel       context.CancelFunc
	workers      sync.WaitGroup
	closed       bool
	jobs         map[string]Job
	activeByRec  map[string]string
	cancelByID   map[string]context.CancelFunc
	running      int
	changed      chan struct{}
}

// Open initializes an optional remux-only service. Missing FFmpeg is not a
// startup error: Available reports false and Start returns ErrUnavailable.
// root is the application data directory, not the canonical recordings path.
func Open(root string, store *storage.Store, ffmpegPath string, concurrency int) (*Service, error) {
	if strings.TrimSpace(root) == "" || store == nil {
		return nil, ErrInvalid
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("prepare export root: %w", err)
	}
	resolved := ""
	if ffmpegPath == "" {
		resolved, err = exec.LookPath(defaultFFmpegBin)
		if err != nil {
			err = nil
		}
	} else {
		if !filepath.IsAbs(ffmpegPath) {
			return nil, fmt.Errorf("ffmpeg path must be absolute")
		}
		resolved = filepath.Clean(ffmpegPath)
	}
	if resolved != "" {
		resolved, err = filepath.EvalSymlinks(resolved)
		if err == nil {
			resolved, err = filepath.Abs(resolved)
		}
		if err != nil {
			return nil, fmt.Errorf("resolve ffmpeg path: %w", err)
		}
		info, statErr := os.Stat(resolved)
		if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
			resolved = ""
		}
	}
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > maxWorkers {
		concurrency = maxWorkers
	}
	managementDir := filepath.Join(absRoot, "management", "exports")
	jobsDir := filepath.Join(absRoot, "exports")
	thumbnailDir := filepath.Join(absRoot, "thumbnails")
	for _, dir := range []string{managementDir, jobsDir, thumbnailDir} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, fmt.Errorf("create export storage: %w", err)
		}
		if err := os.Chmod(dir, 0700); err != nil {
			return nil, fmt.Errorf("restrict export storage: %w", err)
		}
	}
	for _, dir := range []string{absRoot, filepath.Dir(managementDir), managementDir, jobsDir, thumbnailDir} {
		if err := syncDir(dir); err != nil {
			return nil, fmt.Errorf("sync export storage: %w", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		root: absRoot, jobsDir: jobsDir, thumbnailDir: thumbnailDir, statePath: filepath.Join(managementDir, "state.json"),
		store: store, ffmpegPath: resolved, timeout: defaultTimeout,
		queue: make(chan queuedJob, maxActiveJobs), ctx: ctx, cancel: cancel,
		jobs: make(map[string]Job), activeByRec: make(map[string]string), cancelByID: make(map[string]context.CancelFunc),
		changed: make(chan struct{}),
	}
	if err := s.load(); err != nil {
		cancel()
		return nil, err
	}
	if err := s.cleanupOrphanDirectories(); err != nil {
		cancel()
		return nil, err
	}
	for i := 0; i < concurrency; i++ {
		s.workers.Add(1)
		go s.worker()
	}
	return s, nil
}

// Available reports whether a validated FFmpeg executable is configured.
func (s *Service) Available() bool { return s != nil && s.ffmpegPath != "" }

// Start enqueues one HLS source-native Matroska remux. The recording metadata
// is cloned before it is retained. Active exports coalesce by recording ID.
func (s *Service) Start(ctx context.Context, recording *domain.Recording) (Job, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return Job{}, err
		}
	}
	if !s.Available() {
		return Job{}, ErrUnavailable
	}
	if recording == nil || !recordingIDPattern.MatchString(recording.ID) {
		return Job{}, ErrInvalid
	}
	if recording.State == domain.StateRecording {
		return Job{}, ErrActive
	}
	if recording.State != domain.StateCompleted && recording.State != domain.StateStopped {
		return Job{}, ErrUnsupported
	}
	copyRecording, err := cloneRecording(recording)
	if err != nil {
		return Job{}, ErrInvalid
	}
	if _, err := s.sourceObjects(copyRecording); err != nil {
		return Job{}, fmt.Errorf("%w: canonical source is unavailable", ErrUnsupported)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Job{}, ErrUnavailable
	}
	if existingID := s.activeByRec[recording.ID]; existingID != "" {
		if existing, ok := s.jobs[existingID]; ok && active(existing.State) {
			return cloneJob(existing), nil
		}
		delete(s.activeByRec, recording.ID)
	}
	if activeCount(s.jobs) >= maxActiveJobs {
		return Job{}, ErrCapacity
	}
	id, err := randomID()
	if err != nil {
		return Job{}, fmt.Errorf("create export job: %w", err)
	}
	job := Job{ID: id, RecordingID: recording.ID, Kind: jobKindExport, State: StateQueued, CreatedAt: time.Now().UTC(), OutputName: "recording-" + recording.ID + ".mkv"}
	s.jobs[id] = job
	s.activeByRec[recording.ID] = id
	s.notifyLocked()
	if err := s.persistLocked(); err != nil {
		delete(s.jobs, id)
		delete(s.activeByRec, recording.ID)
		return Job{}, errors.New("export job could not be persisted")
	}
	select {
	case s.queue <- queuedJob{id: id, recording: copyRecording}:
		return cloneJob(job), nil
	default:
		delete(s.jobs, id)
		delete(s.activeByRec, recording.ID)
		_ = s.persistLocked()
		return Job{}, ErrCapacity
	}
}

// Get returns a copy of one safe public job summary.
func (s *Service) Get(id string) (Job, error) {
	if !jobIDPattern.MatchString(id) {
		return Job{}, ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return cloneJob(job), nil
}

// List returns recent jobs, optionally scoped to a recording ID.
func (s *Service) List(recordingID string) []Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	jobs := make([]Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		if recordingID == "" || job.RecordingID == recordingID {
			jobs = append(jobs, cloneJob(job))
		}
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].CreatedAt.Equal(jobs[j].CreatedAt) {
			return jobs[i].ID > jobs[j].ID
		}
		return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
	})
	return jobs
}

// InProgress reports whether a recording has a queued or running export.
func (s *Service) InProgress(recordingID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.activeByRec[recordingID]
	job, ok := s.jobs[id]
	return ok && active(job.State)
}

// Cancel requests cancellation. A running FFmpeg process is killed by its
// context; a queued job becomes terminal immediately.
func (s *Service) Cancel(id string) (Job, error) {
	if !jobIDPattern.MatchString(id) {
		return Job{}, ErrNotFound
	}
	s.mu.Lock()
	job, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return Job{}, ErrNotFound
	}
	if !active(job.State) {
		s.mu.Unlock()
		return cloneJob(job), nil
	}
	if cancel := s.cancelByID[id]; cancel != nil {
		cancel()
		s.mu.Unlock()
		return cloneJob(job), nil
	}
	now := time.Now().UTC()
	job.State, job.FinishedAt, job.ErrorCode = StateCanceled, &now, "canceled"
	s.jobs[id] = job
	delete(s.activeByRec, job.RecordingID)
	s.notifyLocked()
	err := s.persistLocked()
	s.mu.Unlock()
	_ = os.RemoveAll(filepath.Join(s.jobsDir, id))
	if err != nil {
		return cloneJob(job), errors.New("export cancellation could not be persisted")
	}
	return cloneJob(job), nil
}

// Delete removes a terminal job and its derivative artifact.
func (s *Service) Delete(id string) error {
	if !jobIDPattern.MatchString(id) {
		return ErrNotFound
	}
	s.mu.Lock()
	job, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return ErrNotFound
	}
	if active(job.State) {
		s.mu.Unlock()
		return ErrConflict
	}
	delete(s.jobs, id)
	if err := s.persistLocked(); err != nil {
		s.jobs[id] = job
		_ = s.persistLocked()
		s.mu.Unlock()
		return errors.New("export job deletion could not be persisted")
	}
	s.mu.Unlock()
	if err := os.RemoveAll(filepath.Join(s.jobsDir, id)); err != nil {
		return errors.New("export artifact deletion failed")
	}
	if err := syncDir(s.jobsDir); err != nil {
		return errors.New("export artifact deletion could not be persisted")
	}
	return nil
}

// OpenDownload opens only a completed, regular, private artifact.
func (s *Service) OpenDownload(id string) (*os.File, Job, error) {
	if !jobIDPattern.MatchString(id) {
		return nil, Job{}, ErrNotFound
	}
	s.mu.Lock()
	job, ok := s.jobs[id]
	if !ok || job.State != StateCompleted {
		s.mu.Unlock()
		return nil, Job{}, ErrNotFound
	}
	job = cloneJob(job)
	path := filepath.Join(s.jobsDir, id, "output.mkv")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != job.Size {
		s.mu.Unlock()
		return nil, Job{}, ErrNotFound
	}
	f, err := os.Open(path)
	s.mu.Unlock()
	if err != nil {
		return nil, Job{}, ErrNotFound
	}
	return f, job, nil
}

// WaitForIdle waits for accepted exports to finish their job-state and
// filesystem mutations. It neither closes the service nor prevents new jobs
// from being accepted after the caller observes idle.
func (s *Service) WaitForIdle(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		if len(s.activeByRec) == 0 && s.running == 0 {
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Service) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *Service) finishRunning() {
	s.mu.Lock()
	if s.running > 0 {
		s.running--
	}
	s.notifyLocked()
	s.mu.Unlock()
}

// Close cancels active process work and joins every worker up to ctx's limit.
func (s *Service) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.cancel()
		for _, cancel := range s.cancelByID {
			cancel()
		}
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.workers.Wait(); close(done) }()
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) worker() {
	defer s.workers.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case task := <-s.queue:
			s.run(task)
		}
	}
}

func (s *Service) run(task queuedJob) {
	s.mu.Lock()
	job, ok := s.jobs[task.id]
	if !ok || job.State != StateQueued || s.closed {
		s.mu.Unlock()
		return
	}
	now := time.Now().UTC()
	job.State, job.StartedAt = StateRunning, &now
	s.jobs[job.ID] = job
	s.running++
	s.notifyLocked()
	defer s.finishRunning()
	jobCtx, cancel := context.WithTimeout(s.ctx, s.timeout)
	s.cancelByID[job.ID] = cancel
	if err := s.persistLocked(); err != nil {
		cancel()
		delete(s.cancelByID, job.ID)
		job.State, job.FinishedAt, job.ErrorCode = StateFailed, timePtr(time.Now().UTC()), "state_persistence_failed"
		s.jobs[job.ID] = job
		delete(s.activeByRec, job.RecordingID)
		_ = s.persistLocked()
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	errCode, size := s.export(jobCtx, task.id, task.recording)
	if errCode == "" {
		if workErr := jobCtx.Err(); workErr != nil {
			errCode = contextErrorCode(workErr)
		} else {
			errCode = s.publish(task.id)
		}
	}

	s.mu.Lock()
	current, exists := s.jobs[job.ID]
	if exists && current.State == StateRunning {
		// Cancel and Close serialize on s.mu. Recheck here so cancellation
		// racing artifact publication cannot report a completed export.
		if workErr := jobCtx.Err(); workErr != nil {
			errCode = contextErrorCode(workErr)
		}
		finished := time.Now().UTC()
		current.FinishedAt = &finished
		if errCode == "" {
			current.State, current.Size = StateCompleted, size
			current.ErrorCode = ""
		} else if errCode == "canceled" {
			current.State, current.ErrorCode = StateCanceled, errCode
		} else {
			current.State, current.ErrorCode = StateFailed, errCode
		}
		s.jobs[job.ID] = current
		delete(s.activeByRec, job.RecordingID)
		if persistErr := s.persistLocked(); persistErr != nil {
			current.State, current.ErrorCode = StateFailed, "state_persistence_failed"
			s.jobs[job.ID] = current
			_ = s.persistLocked()
			errCode = "state_persistence_failed"
		}
	}
	delete(s.cancelByID, job.ID)
	s.mu.Unlock()
	cancel()
	if errCode != "" {
		_ = os.RemoveAll(filepath.Join(s.jobsDir, task.id))
	}
}

func contextErrorCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "canceled"
}

func (s *Service) export(ctx context.Context, id string, recording *domain.Recording) (string, int64) {
	if err := ctx.Err(); err != nil {
		return "canceled", 0
	}
	objects, err := s.sourceObjects(recording)
	if err != nil {
		return "source_unavailable", 0
	}
	jobDir := filepath.Join(s.jobsDir, id)
	if err := os.Mkdir(jobDir, 0700); err != nil {
		return "staging_failed", 0
	}
	workDir := filepath.Join(jobDir, ".work")
	if err := os.Mkdir(workDir, 0700); err != nil {
		_ = os.RemoveAll(jobDir)
		return "staging_failed", 0
	}
	localNames, err := s.prepareInputs(ctx, recording, objects, workDir)
	if err != nil {
		_ = os.RemoveAll(jobDir)
		if errors.Is(err, context.Canceled) {
			return "canceled", 0
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return "timeout", 0
		}
		return "source_unavailable", 0
	}
	playlist, err := buildPlaylist(recording, localNames)
	if err != nil {
		_ = os.RemoveAll(jobDir)
		return "unsupported_media", 0
	}
	playlistPath := filepath.Join(workDir, "input.m3u8")
	if err := writePrivateFile(playlistPath, []byte(playlist)); err != nil {
		_ = os.RemoveAll(jobDir)
		return "staging_failed", 0
	}
	command := exec.CommandContext(ctx, s.ffmpegPath,
		"-nostdin", "-hide_banner", "-loglevel", "error",
		"-protocol_whitelist", "file", "-i", "input.m3u8",
		"-map", "0", "-c", "copy", "-f", "matroska", "output.mkv")
	command.Dir = workDir
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Run(); err != nil {
		_ = os.RemoveAll(jobDir)
		if errors.Is(ctx.Err(), context.Canceled) {
			return "canceled", 0
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "timeout", 0
		}
		return "ffmpeg_failed", 0
	}
	output := filepath.Join(workDir, "output.mkv")
	info, err := os.Lstat(output)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		_ = os.RemoveAll(jobDir)
		return "invalid_output", 0
	}
	if err := os.Chmod(output, 0600); err != nil {
		_ = os.RemoveAll(jobDir)
		return "artifact_sync_failed", 0
	}
	f, err := os.OpenFile(output, os.O_RDWR, 0600)
	if err != nil {
		_ = os.RemoveAll(jobDir)
		return "invalid_output", 0
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		_ = os.RemoveAll(jobDir)
		return "artifact_sync_failed", 0
	}
	return "", info.Size()
}

func (s *Service) publish(id string) string {
	jobDir := filepath.Join(s.jobsDir, id)
	workDir := filepath.Join(jobDir, ".work")
	if err := os.Rename(filepath.Join(workDir, "output.mkv"), filepath.Join(jobDir, "output.mkv")); err != nil {
		return "artifact_publish_failed"
	}
	if err := syncDir(jobDir); err != nil {
		return "artifact_publish_failed"
	}
	if err := syncDir(s.jobsDir); err != nil {
		return "artifact_publish_failed"
	}
	if err := os.RemoveAll(workDir); err != nil {
		return "staging_cleanup_failed"
	}
	if err := syncDir(jobDir); err != nil {
		return "artifact_publish_failed"
	}
	return ""
}

func (s *Service) sourceObjects(recording *domain.Recording) ([]storage.ArchiveEntry, error) {
	if recording == nil {
		return nil, ErrInvalid
	}
	index, err := s.store.ArchiveIndex(recording)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]storage.ArchiveEntry, len(index))
	for _, entry := range index {
		allowed[entry.Path] = entry
	}
	track, err := primaryTrack(recording)
	if err != nil {
		return nil, err
	}
	objects := make([]storage.ArchiveEntry, 0)
	neededInit := map[string]bool{}
	for _, segment := range track.Segments {
		if segment.InitSegmentID != "" {
			neededInit[segment.InitSegmentID] = true
		}
	}
	for _, segment := range track.InitSegments {
		if neededInit[segment.ID] {
			objects = append(objects, storage.ArchiveEntry{Path: segment.StoragePath, Kind: "init_segment", Size: segment.PayloadSize, SHA256: segment.SHA256})
		}
	}
	for _, segment := range track.Segments {
		objects = append(objects, storage.ArchiveEntry{Path: segment.StoragePath, Kind: "segment", Size: segment.PayloadSize, SHA256: segment.SHA256})
	}
	seen := map[string]bool{}
	var total int64
	for _, object := range objects {
		canonical, ok := allowed[object.Path]
		if !ok || seen[object.Path] || canonical.Size != object.Size || canonical.SHA256 != object.SHA256 || object.Size < 0 {
			return nil, ErrInvalid
		}
		seen[object.Path] = true
		if object.Size > maxInputBytes-total {
			return nil, ErrInvalid
		}
		total += object.Size
	}
	return objects, nil
}

func (s *Service) prepareInputs(ctx context.Context, recording *domain.Recording, objects []storage.ArchiveEntry, workDir string) (map[string]string, error) {
	localNames := make(map[string]string, len(objects))
	extensions := sourceExtensions(recording)
	for index, object := range objects {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		file, err := s.store.OpenPayloadReader(recording.ID, object.Path)
		if err != nil {
			return nil, err
		}
		extension := extensions[object.Path]
		if extension == "" {
			_ = file.Close()
			return nil, ErrInvalid
		}
		name := fmt.Sprintf("object-%06d%s", index+1, extension)
		destination, createErr := os.OpenFile(filepath.Join(workDir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if createErr != nil {
			_ = file.Close()
			return nil, createErr
		}
		hash := sha256.New()
		size, copyErr := io.Copy(io.MultiWriter(destination, hash), contextReader{ctx: ctx, r: file})
		closeSourceErr := file.Close()
		if copyErr == nil {
			copyErr = destination.Sync()
		}
		closeDestinationErr := destination.Close()
		if copyErr != nil {
			return nil, copyErr
		}
		if closeSourceErr != nil || closeDestinationErr != nil || size != object.Size || hex.EncodeToString(hash.Sum(nil)) != object.SHA256 {
			return nil, errors.New("source integrity mismatch")
		}
		localNames[object.Path] = name
	}
	return localNames, nil
}

func sourceExtensions(recording *domain.Recording) map[string]string {
	track, err := primaryTrack(recording)
	if err != nil {
		return nil
	}
	extensions := make(map[string]string, len(track.InitSegments)+len(track.Segments))
	for _, init := range track.InitSegments {
		if init.StoragePath != "" {
			extensions[init.StoragePath] = ".mp4"
		}
	}
	for _, segment := range track.Segments {
		if segment.StoragePath == "" {
			continue
		}
		extension := safeMediaExtension(segment.SourceURI)
		if segment.InitSegmentID != "" && extension == ".ts" {
			extension = ".m4s"
		}
		extensions[segment.StoragePath] = extension
	}
	return extensions
}

func safeMediaExtension(sourceURI string) string {
	parsed, err := url.Parse(sourceURI)
	if err != nil {
		return ".ts"
	}
	extension := strings.ToLower(pathpkg.Ext(parsed.Path))
	switch extension {
	case ".3gp", ".aac", ".ac3", ".avi", ".ec3", ".fmp4", ".flac", ".m4a", ".m4s", ".m4v", ".mkv", ".mov", ".mp2", ".mp3", ".mp4", ".mpeg", ".mpg", ".ogg", ".oga", ".ogv", ".ts", ".vob", ".vtt", ".wav", ".webvtt":
		return extension
	default:
		return ".ts"
	}
}

func (s *Service) load() error {
	data, err := os.ReadFile(s.statePath)
	if errors.Is(err, os.ErrNotExist) {
		return s.persist()
	}
	if err != nil {
		return errors.New("export state unavailable")
	}
	if err := os.Chmod(s.statePath, 0600); err != nil {
		return errors.New("export state permissions unavailable")
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil || state.Version != 1 {
		return errors.New("export state invalid")
	}
	for _, job := range state.Jobs {
		if !jobIDPattern.MatchString(job.ID) || !recordingIDPattern.MatchString(job.RecordingID) || job.Kind != jobKindExport {
			continue
		}
		if job.OutputName != "recording-"+job.RecordingID+".mkv" {
			job.OutputName = "recording-" + job.RecordingID + ".mkv"
		}
		if active(job.State) {
			job.State, job.FinishedAt, job.ErrorCode = StateFailed, timePtr(time.Now().UTC()), "interrupted_by_restart"
		} else if job.State == StateCompleted && !s.validArtifact(job) {
			job.State, job.ErrorCode = StateFailed, "artifact_unavailable"
		} else if job.State != StateCompleted && job.State != StateFailed && job.State != StateCanceled {
			job.State, job.FinishedAt, job.ErrorCode = StateFailed, timePtr(time.Now().UTC()), "invalid_persisted_state"
		}
		jobDir := filepath.Join(s.jobsDir, job.ID)
		if job.State == StateCompleted {
			if err := os.Chmod(jobDir, 0700); err != nil {
				return errors.New("completed artifact permissions unavailable")
			}
			if err := os.Chmod(filepath.Join(jobDir, "output.mkv"), 0600); err != nil {
				return errors.New("completed artifact permissions unavailable")
			}
			if err := os.RemoveAll(filepath.Join(jobDir, ".work")); err != nil {
				return errors.New("staging recovery failed")
			}
		} else if err := os.RemoveAll(jobDir); err != nil {
			return errors.New("partial export recovery failed")
		}
		s.jobs[job.ID] = job
	}
	return s.persist()
}

func (s *Service) validArtifact(job Job) bool {
	dir := filepath.Join(s.jobsDir, job.ID)
	dirInfo, err := os.Lstat(dir)
	if err != nil || !dirInfo.IsDir() || dirInfo.Mode()&os.ModeSymlink != 0 {
		return false
	}
	path := filepath.Join(dir, "output.mkv")
	info, err := os.Lstat(path)
	return err == nil && info.Mode().IsRegular() && info.Size() == job.Size && info.Size() > 0
}

func (s *Service) cleanupOrphanDirectories() error {
	entries, err := os.ReadDir(s.jobsDir)
	if err != nil {
		return errors.New("export directory unavailable")
	}
	for _, entry := range entries {
		id := entry.Name()
		if !jobIDPattern.MatchString(id) {
			continue
		}
		s.mu.Lock()
		_, known := s.jobs[id]
		s.mu.Unlock()
		if !known {
			if err := os.RemoveAll(filepath.Join(s.jobsDir, id)); err != nil {
				return errors.New("orphan export cleanup failed")
			}
		}
	}
	return nil
}

func (s *Service) persist() error { s.mu.Lock(); defer s.mu.Unlock(); return s.persistLocked() }

func (s *Service) persistLocked() error {
	jobs := make([]Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, cloneJob(job))
	}
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].CreatedAt.Equal(jobs[j].CreatedAt) {
			return jobs[i].ID < jobs[j].ID
		}
		return jobs[i].CreatedAt.Before(jobs[j].CreatedAt)
	})
	for len(jobs) > maxHistory {
		old := jobs[0]
		if active(old.State) {
			return ErrCapacity
		}
		delete(s.jobs, old.ID)
		jobs = jobs[1:]
		_ = os.RemoveAll(filepath.Join(s.jobsDir, old.ID))
	}
	data, err := json.MarshalIndent(persistedState{Version: 1, Jobs: jobs}, "", "  ")
	if err != nil {
		return err
	}
	return atomicWrite(s.statePath, append(data, '\n'), 0600)
}

func active(state State) bool { return state == StateQueued || state == StateRunning }

func activeCount(jobs map[string]Job) int {
	count := 0
	for _, job := range jobs {
		if active(job.State) {
			count++
		}
	}
	return count
}

func cloneJob(job Job) Job {
	if job.StartedAt != nil {
		value := *job.StartedAt
		job.StartedAt = &value
	}
	if job.FinishedAt != nil {
		value := *job.FinishedAt
		job.FinishedAt = &value
	}
	return job
}

func cloneRecording(recording *domain.Recording) (*domain.Recording, error) {
	data, err := json.Marshal(recording)
	if err != nil {
		return nil, err
	}
	var clone domain.Recording
	if err := json.Unmarshal(data, &clone); err != nil {
		return nil, err
	}
	if clone.Tracks == nil {
		clone.Tracks = map[string]*domain.Track{}
	}
	return &clone, nil
}

func randomID() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(data[:]), nil
}

func timePtr(value time.Time) *time.Time { return &value }

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	if err := os.Chmod(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".state-*.tmp")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if err = f.Chmod(mode); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(temp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func writePrivateFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
