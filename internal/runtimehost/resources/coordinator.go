// Package resources provides process-shareable admission accounting for
// generation-local recorder work. The package is transport-independent; a
// Runtime Host can expose a Coordinator over authenticated local IPC.
package resources

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

const (
	// MaxGlobalBufferBytes and MaxPerRecordingBufferBytes mirror the validated
	// storage ingest limits. Keep these bounds aligned with storage settings.
	MaxGlobalBufferBytes       int64 = 2 << 30
	MaxPerRecordingBufferBytes int64 = 1536 << 20
	MaxQueueObjects                  = 128
	MaxWriterConcurrency             = 1
	// MaxHistoricalScratchObjects and MaxHistoricalScratchBytes bound the
	// Runtime Host temporary disk space used by parallel historical fetches.
	MaxHistoricalScratchObjects           = 4
	MaxHistoricalScratchBytes       int64 = 2 << 30
	DefaultHistoricalScratchObjects       = MaxHistoricalScratchObjects
	DefaultHistoricalScratchBytes   int64 = MaxHistoricalScratchBytes
	maxTokenLength                        = 128
	defaultTelemetryInterval              = 5 * time.Second
	defaultTelemetryRetention             = 24 * time.Hour
	maxTelemetryProjectionSamples         = 24000
)

var (
	ErrInvalidLimits                 = errors.New("invalid runtime resource limits")
	ErrInvalidIdentity               = errors.New("invalid runtime resource identity")
	ErrLeaseMismatch                 = errors.New("runtime resource lease identity mismatch")
	ErrInvalidReservation            = errors.New("reservation size must be greater than zero")
	ErrInvalidHistoricalScratchLease = errors.New("historical scratch lease is invalid")
	ErrInvalidTelemetryGauge         = errors.New("runtime storage telemetry gauge is invalid")
	ErrTelemetryRegression           = errors.New("runtime storage telemetry counters cannot decrease")
	ErrTelemetryOverflow             = errors.New("runtime storage telemetry counter overflow")
)

// Limits are the process-wide resource limits enforced by the Runtime Host.
// WriterConcurrency is intentionally fixed at one to preserve canonical
// storage publication ordering across overlapping generations.
type Limits struct {
	GlobalBufferBytes        int64 `json:"global_buffer_bytes"`
	PerRecordingBufferBytes  int64 `json:"per_recording_buffer_bytes"`
	QueueObjects             int   `json:"queue_objects"`
	WriterConcurrency        int   `json:"writer_concurrency"`
	HistoricalScratchObjects int   `json:"historical_scratch_objects"`
	HistoricalScratchBytes   int64 `json:"historical_scratch_bytes"`
}

// Validate rejects limits that exceed the existing storage ingest contract.
func (l Limits) Validate() error {
	if l.GlobalBufferBytes <= 0 || l.GlobalBufferBytes > MaxGlobalBufferBytes {
		return fmt.Errorf("%w: global buffer must be between 1 byte and 2 GiB", ErrInvalidLimits)
	}
	if l.PerRecordingBufferBytes <= 0 || l.PerRecordingBufferBytes > MaxPerRecordingBufferBytes {
		return fmt.Errorf("%w: per-recording buffer must be between 1 byte and 1536 MiB", ErrInvalidLimits)
	}
	if l.PerRecordingBufferBytes > l.GlobalBufferBytes {
		return fmt.Errorf("%w: per-recording buffer cannot exceed global buffer", ErrInvalidLimits)
	}
	if l.QueueObjects < 1 || l.QueueObjects > MaxQueueObjects {
		return fmt.Errorf("%w: queue capacity must be between 1 and %d objects", ErrInvalidLimits, MaxQueueObjects)
	}
	if l.WriterConcurrency != 1 {
		return fmt.Errorf("%w: writer concurrency must be 1", ErrInvalidLimits)
	}
	if l.HistoricalScratchObjects < 0 || l.HistoricalScratchObjects > MaxHistoricalScratchObjects {
		return fmt.Errorf("%w: historical scratch objects must be between 0 and %d", ErrInvalidLimits, MaxHistoricalScratchObjects)
	}
	if l.HistoricalScratchBytes < 0 || l.HistoricalScratchBytes > MaxHistoricalScratchBytes {
		return fmt.Errorf("%w: historical scratch bytes must be between 0 and %d", ErrInvalidLimits, MaxHistoricalScratchBytes)
	}
	return nil
}

func (l Limits) withDefaults() Limits {
	if l.HistoricalScratchObjects == 0 {
		l.HistoricalScratchObjects = DefaultHistoricalScratchObjects
	}
	if l.HistoricalScratchBytes == 0 {
		l.HistoricalScratchBytes = DefaultHistoricalScratchBytes
	}
	return l
}

// Snapshot contains aggregate coordinator usage only. It never includes owner,
// recording, reservation, queue, writer, or scratch lease identities.
type Snapshot struct {
	Limits                       Limits  `json:"limits"`
	UsedBytes                    int64   `json:"used_bytes"`
	QueueObjects                 int     `json:"queue_objects"`
	QueueBytes                   int64   `json:"queue_bytes"`
	OldestAgeSeconds             float64 `json:"oldest_age_seconds"`
	ActiveWriters                int     `json:"active_writers"`
	HistoricalScratchUsedObjects int     `json:"historical_scratch_used_objects"`
	HistoricalScratchUsedBytes   int64   `json:"historical_scratch_used_bytes"`
}

type reservation struct {
	ownerID     string
	recordingID string
	bytes       int64
}

type lease struct {
	ownerID string
}

type historicalScratchLease struct {
	ownerID string
	objects int
	bytes   int64
}

type telemetryOwner struct {
	readBytes          uint64
	writeBytes         uint64
	errors             uint64
	queueBytes         int64
	oldestQueueAgeSecs float64
}

// Coordinator atomically accounts for RAM reservations, pending persistence
// jobs, and storage writer permits across Runtime Host clients. A lease has no
// wall-clock expiry: callers must release it explicitly or the Host must call
// ReleaseOwner only after confirming that the owning child process is dead.
type Coordinator struct {
	mu sync.Mutex

	limits Limits

	reservations             map[string]reservation
	recordingBytes           map[string]int64
	usedBytes                int64
	queue                    map[string]lease
	writers                  map[string]lease
	historicalScratch        map[string]historicalScratchLease
	historicalScratchObjects int
	historicalScratchBytes   int64
	changed                  chan struct{}

	telemetryInterval    time.Duration
	telemetryRetention   time.Duration
	telemetryMax         int
	telemetryOwners      map[string]telemetryOwner
	telemetryReadTotal   uint64
	telemetryWriteTotal  uint64
	telemetryErrorsTotal uint64
	pendingReadBytes     uint64
	pendingWriteBytes    uint64
	lastTelemetryAt      time.Time
	currentTelemetry     TelemetryThroughput
	telemetrySamples     []TelemetrySample
	telemetryStart       int
	telemetryCount       int
	telemetryStop        chan struct{}
	telemetryDone        chan struct{}
	telemetryCloseOnce   sync.Once
}

// New constructs a coordinator with immutable process-wide limits.
func New(limits Limits) (*Coordinator, error) {
	return newCoordinator(limits, defaultTelemetryInterval, defaultTelemetryRetention, false)
}

// NewWithTelemetry constructs a process-wide resource coordinator and starts
// its bounded Host-owned storage I/O sampler. The sampler owns only aggregate
// recorder I/O counters and never stores recording IDs or filesystem paths.
func NewWithTelemetry(limits Limits, sampleInterval, retention time.Duration) (*Coordinator, error) {
	if sampleInterval <= 0 || retention < sampleInterval || retention > defaultTelemetryRetention {
		return nil, errors.New("runtime telemetry interval or retention is invalid")
	}
	return newCoordinator(limits, sampleInterval, retention, true)
}

func newCoordinator(limits Limits, sampleInterval, retention time.Duration, startSampler bool) (*Coordinator, error) {
	limits = limits.withDefaults()
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	maxSamples := int(retention / sampleInterval)
	if retention%sampleInterval != 0 {
		maxSamples++
	}
	if maxSamples < 1 {
		maxSamples = 1
	}
	c := &Coordinator{
		limits:            limits,
		reservations:      make(map[string]reservation),
		recordingBytes:    make(map[string]int64),
		queue:             make(map[string]lease),
		writers:           make(map[string]lease),
		historicalScratch: make(map[string]historicalScratchLease),
		changed:           make(chan struct{}),
		telemetryInterval: sampleInterval, telemetryRetention: retention,
		telemetryMax: maxSamples, telemetryOwners: make(map[string]telemetryOwner),
		telemetrySamples: make([]TelemetrySample, maxSamples),
	}
	if startSampler {
		c.telemetryStop = make(chan struct{})
		c.telemetryDone = make(chan struct{})
		c.lastTelemetryAt = time.Now()
		go c.runTelemetrySampler()
	}
	return c, nil
}

// TelemetrySample is one Host-owned aggregate storage I/O sample. It contains
// no owner or recording identity. ObservedForNanos is used only to retain the
// five-minute active-observation ceiling semantics across cadence jitter.
type TelemetrySample struct {
	At                  time.Time `json:"at"`
	ReadBytesPerSecond  uint64    `json:"read_bytes_per_second"`
	WriteBytesPerSecond uint64    `json:"write_bytes_per_second"`
	BufferUsedBytes     int64     `json:"buffer_used_bytes"`
	PersistQueueBytes   int64     `json:"persist_queue_bytes"`
	ObservedForNanos    int64     `json:"-"`
}

// TelemetryThroughput contains cumulative Host-wide recorder I/O totals and
// the most recent aggregate sample's rate. Latency remains process-local in
// storage because the authenticated reporting contract intentionally carries
// only byte and error counters.
type TelemetryThroughput struct {
	ReadBytesPerSecond  uint64 `json:"read_bytes_per_second"`
	WriteBytesPerSecond uint64 `json:"write_bytes_per_second"`
	ReadBytesTotal      uint64 `json:"read_bytes_total"`
	WriteBytesTotal     uint64 `json:"write_bytes_total"`
}

type TelemetryCeiling struct {
	ReadBytesPerSecond  uint64 `json:"read_bytes_per_second,omitempty"`
	WriteBytesPerSecond uint64 `json:"write_bytes_per_second,omitempty"`
	Source              string `json:"source"`
}

// TelemetrySnapshot is the bounded aggregate projection returned to managed
// application generations. The ring itself remains private to Runtime Host.
type TelemetrySnapshot struct {
	Throughput       TelemetryThroughput `json:"throughput"`
	EstimatedCeiling TelemetryCeiling    `json:"estimated_ceiling"`
	ErrorsTotal      uint64              `json:"errors_total"`
	Samples          []TelemetrySample   `json:"samples"`
}

// ReportTelemetry replaces one process owner's cumulative totals. Duplicate
// reports are idempotent; a counter reset or arithmetic overflow is rejected
// without changing that owner's baseline or aggregate counters.
func (c *Coordinator) ReportTelemetry(ownerID string, readBytes, writeBytes, errorsTotal uint64) error {
	return c.ReportProcessTelemetry(ownerID, readBytes, writeBytes, errorsTotal, 0, 0)
}

// ReportProcessTelemetry replaces one process owner's cumulative counters and
// latest queue gauges. Cumulative counters are delta-aggregated exactly once;
// queue gauges are combined across currently live owners at read/sample time.
func (c *Coordinator) ReportProcessTelemetry(ownerID string, readBytes, writeBytes, errorsTotal uint64, queueBytes int64, oldestQueueAgeSeconds float64) error {
	if c == nil || validateIDs(ownerID) != nil {
		return ErrInvalidIdentity
	}
	if queueBytes < 0 || math.IsNaN(oldestQueueAgeSeconds) || math.IsInf(oldestQueueAgeSeconds, 0) || oldestQueueAgeSeconds < 0 {
		return ErrInvalidTelemetryGauge
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if queueBytes > c.limits.GlobalBufferBytes {
		return ErrInvalidTelemetryGauge
	}
	previous, exists := c.telemetryOwners[ownerID]
	if exists && (readBytes < previous.readBytes || writeBytes < previous.writeBytes || errorsTotal < previous.errors) {
		return ErrTelemetryRegression
	}
	readDelta, writeDelta, errorDelta := readBytes, writeBytes, errorsTotal
	if exists {
		readDelta -= previous.readBytes
		writeDelta -= previous.writeBytes
		errorDelta -= previous.errors
	}
	newRead, okRead := checkedAddUint64(c.telemetryReadTotal, readDelta)
	newWrite, okWrite := checkedAddUint64(c.telemetryWriteTotal, writeDelta)
	newErrors, okErrors := checkedAddUint64(c.telemetryErrorsTotal, errorDelta)
	pendingRead, okPendingRead := checkedAddUint64(c.pendingReadBytes, readDelta)
	pendingWrite, okPendingWrite := checkedAddUint64(c.pendingWriteBytes, writeDelta)
	if !okRead || !okWrite || !okErrors || !okPendingRead || !okPendingWrite {
		return ErrTelemetryOverflow
	}
	var otherQueueBytes int64
	for owner, value := range c.telemetryOwners {
		if owner == ownerID || value.queueBytes == 0 {
			continue
		}
		if otherQueueBytes > c.limits.GlobalBufferBytes-value.queueBytes {
			return ErrInvalidTelemetryGauge
		}
		otherQueueBytes += value.queueBytes
	}
	if queueBytes > c.limits.GlobalBufferBytes-otherQueueBytes {
		return ErrInvalidTelemetryGauge
	}
	c.telemetryOwners[ownerID] = telemetryOwner{readBytes: readBytes, writeBytes: writeBytes, errors: errorsTotal, queueBytes: queueBytes, oldestQueueAgeSecs: oldestQueueAgeSeconds}
	c.telemetryReadTotal, c.telemetryWriteTotal, c.telemetryErrorsTotal = newRead, newWrite, newErrors
	c.pendingReadBytes, c.pendingWriteBytes = pendingRead, pendingWrite
	return nil
}

func checkedAddUint64(left, right uint64) (uint64, bool) {
	if math.MaxUint64-left < right {
		return 0, false
	}
	return left + right, true
}

// SampleTelemetryAt advances aggregate sampling. It is exported so the Host
// sampler and deterministic coordinator tests use the same logic.
func (c *Coordinator) SampleTelemetryAt(now time.Time) {
	if c == nil || now.IsZero() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sampleTelemetryLocked(now)
}

func (c *Coordinator) sampleTelemetryLocked(now time.Time) {
	if c.lastTelemetryAt.IsZero() {
		c.lastTelemetryAt = now
		return
	}
	elapsed := now.Sub(c.lastTelemetryAt)
	if elapsed <= 0 {
		return
	}
	c.currentTelemetry = TelemetryThroughput{
		ReadBytesPerSecond:  rateBytes(c.pendingReadBytes, elapsed),
		WriteBytesPerSecond: rateBytes(c.pendingWriteBytes, elapsed),
		ReadBytesTotal:      c.telemetryReadTotal,
		WriteBytesTotal:     c.telemetryWriteTotal,
	}
	c.appendTelemetrySampleLocked(TelemetrySample{
		At: now.UTC(), ReadBytesPerSecond: c.currentTelemetry.ReadBytesPerSecond,
		WriteBytesPerSecond: c.currentTelemetry.WriteBytesPerSecond,
		BufferUsedBytes:     c.usedBytes, PersistQueueBytes: c.queueBytesLocked(),
		ObservedForNanos: elapsed.Nanoseconds(),
	})
	c.pendingReadBytes, c.pendingWriteBytes = 0, 0
	c.lastTelemetryAt = now
	c.pruneTelemetryLocked(now.Add(-c.telemetryRetention))
}

func rateBytes(bytes uint64, elapsed time.Duration) uint64 {
	if elapsed <= 0 {
		return 0
	}
	rate := float64(bytes) / elapsed.Seconds()
	if rate >= float64(math.MaxUint64) {
		return math.MaxUint64
	}
	return uint64(rate)
}

func (c *Coordinator) appendTelemetrySampleLocked(sample TelemetrySample) {
	if len(c.telemetrySamples) == 0 {
		return
	}
	if c.telemetryCount < len(c.telemetrySamples) {
		index := (c.telemetryStart + c.telemetryCount) % len(c.telemetrySamples)
		c.telemetrySamples[index] = sample
		c.telemetryCount++
		return
	}
	c.telemetrySamples[c.telemetryStart] = sample
	c.telemetryStart = (c.telemetryStart + 1) % len(c.telemetrySamples)
}

func (c *Coordinator) pruneTelemetryLocked(cutoff time.Time) {
	for c.telemetryCount > 0 && c.telemetrySamples[c.telemetryStart].At.Before(cutoff) {
		c.telemetryStart = (c.telemetryStart + 1) % len(c.telemetrySamples)
		c.telemetryCount--
	}
}

// TelemetrySnapshot returns aggregate totals, the latest rates, a p95 observed
// ceiling, and a bounded chronological projection. At high-frequency custom
// cadences the IPC projection is evenly thinned to stay within the framed IPC
// response bound; the Host ring and ceiling calculation retain full cadence.
func (c *Coordinator) TelemetrySnapshot() TelemetrySnapshot {
	if c == nil {
		return TelemetrySnapshot{EstimatedCeiling: TelemetryCeiling{Source: "unknown"}, Samples: []TelemetrySample{}}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	samples := c.chronologicalTelemetryLocked()
	ceiling := estimateTelemetryCeiling(samples)
	projected := boundTelemetryProjection(samples, maxTelemetryProjectionSamples)
	throughput := c.currentTelemetry
	throughput.ReadBytesTotal = c.telemetryReadTotal
	throughput.WriteBytesTotal = c.telemetryWriteTotal
	return TelemetrySnapshot{
		Throughput: throughput, EstimatedCeiling: ceiling,
		ErrorsTotal: c.telemetryErrorsTotal, Samples: projected,
	}
}

func (c *Coordinator) chronologicalTelemetryLocked() []TelemetrySample {
	result := make([]TelemetrySample, c.telemetryCount)
	for i := range result {
		result[i] = c.telemetrySamples[(c.telemetryStart+i)%len(c.telemetrySamples)]
	}
	return result
}

func estimateTelemetryCeiling(samples []TelemetrySample) TelemetryCeiling {
	reads := make([]uint64, 0, len(samples))
	writes := make([]uint64, 0, len(samples))
	var active time.Duration
	for _, sample := range samples {
		if sample.ReadBytesPerSecond > 0 {
			reads = append(reads, sample.ReadBytesPerSecond)
		}
		if sample.WriteBytesPerSecond > 0 {
			writes = append(writes, sample.WriteBytesPerSecond)
		}
		if sample.ReadBytesPerSecond > 0 || sample.WriteBytesPerSecond > 0 {
			observed := time.Duration(sample.ObservedForNanos)
			if observed > 0 {
				remaining := 5*time.Minute - active
				if observed >= remaining {
					active = 5 * time.Minute
				} else {
					active += observed
				}
			}
		}
	}
	if active < 5*time.Minute {
		return TelemetryCeiling{Source: "unknown"}
	}
	result := TelemetryCeiling{Source: "observed"}
	if len(reads) >= 20 {
		result.ReadBytesPerSecond = percentile95(reads)
	}
	if len(writes) >= 20 {
		result.WriteBytesPerSecond = percentile95(writes)
	}
	if result.ReadBytesPerSecond == 0 && result.WriteBytesPerSecond == 0 {
		return TelemetryCeiling{Source: "unknown"}
	}
	return result
}

func percentile95(values []uint64) uint64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]uint64(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := int(math.Ceil(float64(len(ordered))*.95)) - 1
	if index < 0 {
		index = 0
	}
	return ordered[index]
}

func boundTelemetryProjection(samples []TelemetrySample, maxSamples int) []TelemetrySample {
	if len(samples) <= maxSamples {
		return append([]TelemetrySample(nil), samples...)
	}
	if maxSamples <= 1 {
		return append([]TelemetrySample(nil), samples[len(samples)-1:]...)
	}
	result := make([]TelemetrySample, 0, maxSamples)
	for i := 0; i < maxSamples; i++ {
		index := i * (len(samples) - 1) / (maxSamples - 1)
		result = append(result, samples[index])
	}
	return result
}

func (c *Coordinator) runTelemetrySampler() {
	defer close(c.telemetryDone)
	ticker := time.NewTicker(c.telemetryInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			c.SampleTelemetryAt(now)
		case <-c.telemetryStop:
			return
		}
	}
}

// Close stops only the Host telemetry sampler. Callers should invoke it when
// Runtime Host shuts down; resource lease cleanup remains a separate action.
func (c *Coordinator) Close() {
	if c == nil || c.telemetryStop == nil {
		return
	}
	c.telemetryCloseOnce.Do(func() { close(c.telemetryStop) })
	<-c.telemetryDone
}

// SetReservation sets a payload lease to desiredBytes. Passing the same value
// repeatedly is idempotent. An increase reserves only the additional bytes
// and waits for capacity; a decrease releases the difference immediately.
// desiredBytes must be positive. Reservation IDs are globally unique among
// live leases so identity mismatches can be rejected rather than silently
// accounting bytes against the wrong recording.
func (c *Coordinator) SetReservation(ctx context.Context, ownerID, recordingID, reservationID string, desiredBytes int64) error {
	if err := validateContextAndIDs(ctx, ownerID, recordingID, reservationID); err != nil {
		return err
	}
	if desiredBytes <= 0 {
		return ErrInvalidReservation
	}
	if desiredBytes > c.limits.GlobalBufferBytes || desiredBytes > c.limits.PerRecordingBufferBytes {
		return fmt.Errorf("%w: reservation exceeds configured byte limit", ErrInvalidReservation)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		current, exists := c.reservations[reservationID]
		if exists && (current.ownerID != ownerID || current.recordingID != recordingID) {
			c.mu.Unlock()
			return ErrLeaseMismatch
		}

		if exists && desiredBytes == current.bytes {
			c.mu.Unlock()
			return nil
		}

		oldBytes := int64(0)
		if exists {
			oldBytes = current.bytes
		}
		if desiredBytes < oldBytes {
			c.setReservationLocked(reservationID, reservation{ownerID: ownerID, recordingID: recordingID, bytes: desiredBytes}, oldBytes)
			c.signalLocked()
			c.mu.Unlock()
			return nil
		}

		increase := desiredBytes - oldBytes
		recordingUsed := c.recordingBytes[recordingID]
		if c.usedBytes+increase <= c.limits.GlobalBufferBytes && recordingUsed+increase <= c.limits.PerRecordingBufferBytes {
			c.setReservationLocked(reservationID, reservation{ownerID: ownerID, recordingID: recordingID, bytes: desiredBytes}, oldBytes)
			c.mu.Unlock()
			return nil
		}
		changed := c.changed
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (c *Coordinator) setReservationLocked(id string, next reservation, oldBytes int64) {
	delta := next.bytes - oldBytes
	c.usedBytes += delta
	c.recordingBytes[next.recordingID] += delta
	if c.recordingBytes[next.recordingID] == 0 {
		delete(c.recordingBytes, next.recordingID)
	}
	c.reservations[id] = next
}

// ReleaseReservation removes the complete reservation. Releasing an absent ID
// is idempotent; if the ID is live, an owner/recording mismatch is rejected.
func (c *Coordinator) ReleaseReservation(ownerID, recordingID, reservationID string) error {
	if err := validateIDs(ownerID, recordingID, reservationID); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current, exists := c.reservations[reservationID]
	if !exists {
		return nil
	}
	if current.ownerID != ownerID || current.recordingID != recordingID {
		return ErrLeaseMismatch
	}
	delete(c.reservations, reservationID)
	c.usedBytes -= current.bytes
	c.recordingBytes[recordingID] -= current.bytes
	if c.recordingBytes[recordingID] == 0 {
		delete(c.recordingBytes, recordingID)
	}
	c.signalLocked()
	return nil
}

// AcquireQueue acquires one globally bounded pending-persistence slot. Repeating
// the call while the same owner/job lease is active is idempotent.
func (c *Coordinator) AcquireQueue(ctx context.Context, ownerID, jobID string) error {
	if err := validateContextAndIDs(ctx, ownerID, jobID); err != nil {
		return err
	}
	key := leaseKey(ownerID, jobID)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		if _, exists := c.queue[key]; exists {
			c.mu.Unlock()
			return nil
		}
		if len(c.queue) < c.limits.QueueObjects {
			c.queue[key] = lease{ownerID: ownerID}
			c.mu.Unlock()
			return nil
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// ReleaseQueue releases a pending persistence slot. Releasing an absent lease
// is idempotent.
func (c *Coordinator) ReleaseQueue(ownerID, jobID string) error {
	if err := validateIDs(ownerID, jobID); err != nil {
		return err
	}
	key := leaseKey(ownerID, jobID)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.queue[key]; !exists {
		return nil
	}
	delete(c.queue, key)
	c.signalLocked()
	return nil
}

// AcquireWriter acquires a globally bounded durable storage writer permit.
// Repeating the call while the same owner/writer lease is active is idempotent.
func (c *Coordinator) AcquireWriter(ctx context.Context, ownerID, writerID string) error {
	if err := validateContextAndIDs(ctx, ownerID, writerID); err != nil {
		return err
	}
	key := leaseKey(ownerID, writerID)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		if _, exists := c.writers[key]; exists {
			c.mu.Unlock()
			return nil
		}
		if len(c.writers) < c.limits.WriterConcurrency {
			c.writers[key] = lease{ownerID: ownerID}
			c.mu.Unlock()
			return nil
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// ReleaseWriter releases a storage writer permit. Releasing an absent lease is
// idempotent.
func (c *Coordinator) ReleaseWriter(ownerID, writerID string) error {
	if err := validateIDs(ownerID, writerID); err != nil {
		return err
	}
	key := leaseKey(ownerID, writerID)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.writers[key]; !exists {
		return nil
	}
	delete(c.writers, key)
	c.signalLocked()
	return nil
}

// AcquireHistoricalScratch reserves bounded process-wide scratch capacity for
// one historical fetch. Object and byte capacity are admitted atomically.
// Repeating the exact owner, lease, and size is idempotent.
func (c *Coordinator) AcquireHistoricalScratch(ctx context.Context, ownerID, leaseID string, objects int, bytes int64) error {
	if err := validateContextAndIDs(ctx, ownerID, leaseID); err != nil {
		return err
	}
	if objects <= 0 || bytes <= 0 || objects > c.limits.HistoricalScratchObjects || bytes > c.limits.HistoricalScratchBytes {
		return fmt.Errorf("%w: request exceeds configured capacity", ErrInvalidHistoricalScratchLease)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		key := leaseKey(ownerID, leaseID)
		current, exists := c.historicalScratch[key]
		if exists {
			if current.objects != objects || current.bytes != bytes {
				c.mu.Unlock()
				return ErrLeaseMismatch
			}
			c.mu.Unlock()
			return nil
		}
		if c.historicalScratchObjects <= c.limits.HistoricalScratchObjects-objects && c.historicalScratchBytes <= c.limits.HistoricalScratchBytes-bytes {
			c.historicalScratch[key] = historicalScratchLease{ownerID: ownerID, objects: objects, bytes: bytes}
			c.historicalScratchObjects += objects
			c.historicalScratchBytes += bytes
			c.mu.Unlock()
			return nil
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// ReleaseHistoricalScratch releases one process-wide historical scratch lease.
// Releasing an absent lease is idempotent. A live lease requires its exact owner.
func (c *Coordinator) ReleaseHistoricalScratch(ownerID, leaseID string) error {
	if err := validateIDs(ownerID, leaseID); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := leaseKey(ownerID, leaseID)
	current, exists := c.historicalScratch[key]
	if !exists {
		return nil
	}
	delete(c.historicalScratch, key)
	c.historicalScratchObjects -= current.objects
	c.historicalScratchBytes -= current.bytes
	c.signalLocked()
	return nil
}

// ReleaseOwner reclaims every lease held by ownerID. The supervisor must call
// this only after confirming the child process is dead. It is safe to repeat.
func (c *Coordinator) ReleaseOwner(ownerID string) error {
	if err := validateIDs(ownerID); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	changed := false
	for id, item := range c.reservations {
		if item.ownerID != ownerID {
			continue
		}
		delete(c.reservations, id)
		c.usedBytes -= item.bytes
		c.recordingBytes[item.recordingID] -= item.bytes
		if c.recordingBytes[item.recordingID] == 0 {
			delete(c.recordingBytes, item.recordingID)
		}
		changed = true
	}
	for key, item := range c.queue {
		if item.ownerID == ownerID {
			delete(c.queue, key)
			changed = true
		}
	}
	for key, item := range c.writers {
		if item.ownerID == ownerID {
			delete(c.writers, key)
			changed = true
		}
	}
	for key, item := range c.historicalScratch {
		if item.ownerID == ownerID {
			delete(c.historicalScratch, key)
			c.historicalScratchObjects -= item.objects
			c.historicalScratchBytes -= item.bytes
			changed = true
		}
	}
	delete(c.telemetryOwners, ownerID)
	if changed {
		c.signalLocked()
	}
	return nil
}

// Snapshot returns aggregate usage and configured limits without exposing any
// individual lease identity.
func (c *Coordinator) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	queueBytes, oldestQueueAge := c.queueGaugesLocked()
	return Snapshot{
		Limits: c.limits, UsedBytes: c.usedBytes,
		QueueObjects: len(c.queue), QueueBytes: queueBytes,
		OldestAgeSeconds: oldestQueueAge, ActiveWriters: len(c.writers),
		HistoricalScratchUsedObjects: c.historicalScratchObjects,
		HistoricalScratchUsedBytes:   c.historicalScratchBytes,
	}
}

func (c *Coordinator) queueGaugesLocked() (int64, float64) {
	var queueBytes int64
	var oldestAge float64
	for _, owner := range c.telemetryOwners {
		if owner.queueBytes > 0 && queueBytes <= math.MaxInt64-owner.queueBytes {
			queueBytes += owner.queueBytes
		} else if owner.queueBytes > 0 {
			queueBytes = math.MaxInt64
		}
		if owner.oldestQueueAgeSecs > oldestAge {
			oldestAge = owner.oldestQueueAgeSecs
		}
	}
	return queueBytes, oldestAge
}

func (c *Coordinator) queueBytesLocked() int64 {
	queueBytes, _ := c.queueGaugesLocked()
	return queueBytes
}

func (c *Coordinator) signalLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func validateContextAndIDs(ctx context.Context, ids ...string) error {
	if ctx == nil {
		return errors.New("runtime resource context is required")
	}
	if err := validateIDs(ids...); err != nil {
		return err
	}
	return nil
}

func validateIDs(ids ...string) error {
	for _, id := range ids {
		if len(id) == 0 || len(id) > maxTokenLength || !isAlphaNumeric(id[0]) {
			return ErrInvalidIdentity
		}
		previousDot := false
		for i := 0; i < len(id); i++ {
			b := id[i]
			if isAlphaNumeric(b) || b == '-' || b == '_' {
				previousDot = false
				continue
			}
			if b == '.' && !previousDot {
				previousDot = true
				continue
			}
			return ErrInvalidIdentity
		}
		if previousDot {
			return ErrInvalidIdentity
		}
	}
	return nil
}

func isAlphaNumeric(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9'
}

func leaseKey(ownerID, id string) string {
	// IDs cannot contain NUL, so this separator is unambiguous.
	return ownerID + "\x00" + id
}
