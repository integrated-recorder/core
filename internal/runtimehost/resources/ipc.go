package resources

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"time"

	"github.com/integrated-recorder/core/internal/runtimehost/recordingowner"
	"github.com/integrated-recorder/core/internal/runtimeipc"
)

const (
	// IPCIdentity is stable across application generations because the resource
	// coordinator belongs to Runtime Host rather than to an Engine generation.
	IPCIdentity = "runtime-host-resources"
	ipcTimeout  = 30 * time.Second

	operationSetReservation     = "resource_set_reservation"
	operationReleaseReservation = "resource_release_reservation"
	operationAcquireQueue       = "resource_acquire_queue"
	operationReleaseQueue       = "resource_release_queue"
	operationAcquireWriter      = "resource_acquire_writer"
	operationReleaseWriter      = "resource_release_writer"
	operationSnapshot           = "resource_snapshot"
	operationReportTelemetry    = "resource_report_telemetry"
	operationTelemetrySnapshot  = "resource_telemetry_snapshot"
	operationClaimRecording     = "resource_claim_recording"
	operationReleaseRecording   = "resource_release_recording"
)

var recordingIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

// IPCHandler exposes only bounded resource accounting operations and aggregate
// usage. Owner-wide lease cleanup remains an in-process Host operation that is
// permitted only after the supervisor confirms child process death.
type IPCHandler struct {
	coordinator    *Coordinator
	ownerAuthority *RecordingOwnerAuthority
}

func NewIPCHandler(coordinator *Coordinator) (*IPCHandler, error) {
	if coordinator == nil {
		return nil, errors.New("runtime resource coordinator is required")
	}
	return &IPCHandler{coordinator: coordinator}, nil
}

func NewIPCHandlerWithRecordingOwners(coordinator *Coordinator, authority *RecordingOwnerAuthority) (*IPCHandler, error) {
	if coordinator == nil || authority == nil {
		return nil, errors.New("runtime resource coordinator and recording owner authority are required")
	}
	return &IPCHandler{coordinator: coordinator, ownerAuthority: authority}, nil
}

func (h *IPCHandler) Handle(ctx context.Context, operation string, payload json.RawMessage) (any, error) {
	if h == nil || h.coordinator == nil {
		return nil, resourceIPCError("resource_unavailable", "runtime resource coordinator is unavailable")
	}
	switch operation {
	case operationSetReservation:
		var request reservationRequest
		if err := decodeResourceRequest(payload, &request); err != nil || !validRecordingID(request.RecordingID) || validateIDs(request.OwnerID, request.ReservationID) != nil || request.DesiredBytes <= 0 {
			return nil, resourceIPCError("invalid_request", "reservation request is invalid")
		}
		if err := h.coordinator.SetReservation(ctx, request.OwnerID, request.RecordingID, request.ReservationID, request.DesiredBytes); err != nil {
			return nil, mapResourceError(err)
		}
		return emptyResult{}, nil
	case operationReleaseReservation:
		var request reservationReleaseRequest
		if err := decodeResourceRequest(payload, &request); err != nil || !validRecordingID(request.RecordingID) || validateIDs(request.OwnerID, request.ReservationID) != nil {
			return nil, resourceIPCError("invalid_request", "reservation release request is invalid")
		}
		if err := h.coordinator.ReleaseReservation(request.OwnerID, request.RecordingID, request.ReservationID); err != nil {
			return nil, mapResourceError(err)
		}
		return emptyResult{}, nil
	case operationAcquireQueue:
		var request leaseRequest
		if err := decodeResourceRequest(payload, &request); err != nil || validateIDs(request.OwnerID, request.LeaseID) != nil {
			return nil, resourceIPCError("invalid_request", "queue lease request is invalid")
		}
		if err := h.coordinator.AcquireQueue(ctx, request.OwnerID, request.LeaseID); err != nil {
			return nil, mapResourceError(err)
		}
		return emptyResult{}, nil
	case operationReleaseQueue:
		var request leaseRequest
		if err := decodeResourceRequest(payload, &request); err != nil || validateIDs(request.OwnerID, request.LeaseID) != nil {
			return nil, resourceIPCError("invalid_request", "queue lease release request is invalid")
		}
		if err := h.coordinator.ReleaseQueue(request.OwnerID, request.LeaseID); err != nil {
			return nil, mapResourceError(err)
		}
		return emptyResult{}, nil
	case operationAcquireWriter:
		var request leaseRequest
		if err := decodeResourceRequest(payload, &request); err != nil || validateIDs(request.OwnerID, request.LeaseID) != nil {
			return nil, resourceIPCError("invalid_request", "writer lease request is invalid")
		}
		if err := h.coordinator.AcquireWriter(ctx, request.OwnerID, request.LeaseID); err != nil {
			return nil, mapResourceError(err)
		}
		return emptyResult{}, nil
	case operationReleaseWriter:
		var request leaseRequest
		if err := decodeResourceRequest(payload, &request); err != nil || validateIDs(request.OwnerID, request.LeaseID) != nil {
			return nil, resourceIPCError("invalid_request", "writer lease release request is invalid")
		}
		if err := h.coordinator.ReleaseWriter(request.OwnerID, request.LeaseID); err != nil {
			return nil, mapResourceError(err)
		}
		return emptyResult{}, nil
	case operationSnapshot:
		if len(bytes.TrimSpace(payload)) != 0 && !bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
			return nil, resourceIPCError("invalid_request", "snapshot request must be empty")
		}
		return h.coordinator.Snapshot(), nil
	case operationReportTelemetry:
		var request telemetryReportRequest
		if err := decodeResourceRequest(payload, &request); err != nil || validateIDs(request.OwnerID) != nil {
			return nil, resourceIPCError("invalid_request", "storage telemetry report is invalid")
		}
		if err := h.coordinator.ReportProcessTelemetry(request.OwnerID, request.ReadBytes, request.WriteBytes, request.Errors, request.QueueBytes, request.OldestAgeSeconds); err != nil {
			return nil, mapResourceError(err)
		}
		return emptyResult{}, nil
	case operationTelemetrySnapshot:
		if len(bytes.TrimSpace(payload)) != 0 && !bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
			return nil, resourceIPCError("invalid_request", "telemetry snapshot request must be empty")
		}
		return h.coordinator.TelemetrySnapshot(), nil
	case operationClaimRecording:
		if h.ownerAuthority == nil {
			return nil, resourceIPCError("recording_owner_unavailable", "recording ownership is unavailable")
		}
		var request recordingOwnerClaimRequest
		if err := decodeResourceRequest(payload, &request); err != nil || validateIDs(request.OwnerID) != nil || (request.RecordingID != "" && !validRecordingID(request.RecordingID)) {
			return nil, resourceIPCError("invalid_request", "recording owner request is invalid")
		}
		owner, err := h.ownerAuthority.Claim(request.OwnerID, request.RecordingID)
		if err != nil {
			return nil, ownerIPCError(err)
		}
		return owner, nil
	case operationReleaseRecording:
		if h.ownerAuthority == nil {
			return nil, resourceIPCError("recording_owner_unavailable", "recording ownership is unavailable")
		}
		var request recordingOwnerReleaseRequest
		if err := decodeResourceRequest(payload, &request); err != nil || validateIDs(request.OwnerID) != nil {
			return nil, resourceIPCError("invalid_request", "recording owner release request is invalid")
		}
		if err := h.ownerAuthority.Release(request.OwnerID, request.Owner); err != nil {
			return nil, ownerIPCError(err)
		}
		return emptyResult{}, nil
	default:
		return nil, resourceIPCError("unsupported_operation", "runtime resource operation is unsupported")
	}
}

func (h *IPCHandler) RuntimeInstanceID() string { return "runtime-host-resource-service" }

// NewIPCServer constructs the Runtime Host's authenticated UDS service. The
// generation identity must remain IPCIdentity across application generations.
func NewIPCServer(socketPath string, token []byte, coordinator *Coordinator) (*runtimeipc.Server, error) {
	handler, err := NewIPCHandler(coordinator)
	if err != nil {
		return nil, err
	}
	return runtimeipc.NewServer(socketPath, IPCIdentity, token, handler)
}

func NewIPCServerWithRecordingOwners(socketPath string, token []byte, coordinator *Coordinator, authority *RecordingOwnerAuthority) (*runtimeipc.Server, error) {
	handler, err := NewIPCHandlerWithRecordingOwners(coordinator, authority)
	if err != nil {
		return nil, err
	}
	return runtimeipc.NewServer(socketPath, IPCIdentity, token, handler)
}

type reservationRequest struct {
	OwnerID       string `json:"owner_id"`
	RecordingID   string `json:"recording_id"`
	ReservationID string `json:"reservation_id"`
	DesiredBytes  int64  `json:"desired_bytes"`
}

type recordingOwnerClaimRequest struct {
	OwnerID     string `json:"owner_id"`
	RecordingID string `json:"recording_id,omitempty"`
}

type recordingOwnerReleaseRequest struct {
	OwnerID string               `json:"owner_id"`
	Owner   recordingowner.Owner `json:"owner"`
}

type reservationReleaseRequest struct {
	OwnerID       string `json:"owner_id"`
	RecordingID   string `json:"recording_id"`
	ReservationID string `json:"reservation_id"`
}

type leaseRequest struct {
	OwnerID string `json:"owner_id"`
	LeaseID string `json:"lease_id"`
}

// OwnerID is populated from RuntimeClient's construction-time identity; the
// client API does not permit callers to choose another owner's telemetry.
type telemetryReportRequest struct {
	OwnerID          string  `json:"owner_id"`
	ReadBytes        uint64  `json:"read_bytes"`
	WriteBytes       uint64  `json:"write_bytes"`
	Errors           uint64  `json:"errors"`
	QueueBytes       int64   `json:"queue_bytes"`
	OldestAgeSeconds float64 `json:"oldest_age_seconds"`
}

type emptyResult struct{}

type resourceError struct {
	code    string
	message string
}

func (e *resourceError) Error() string                    { return e.message }
func (e *resourceError) PublicIPCError() (string, string) { return e.code, e.message }

func resourceIPCError(code, message string) error {
	return &resourceError{code: code, message: message}
}

func mapResourceError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return err
	}
	return resourceIPCError("resource_operation_failed", "runtime resource operation failed")
}

func decodeResourceRequest(raw json.RawMessage, dst any) error {
	if len(raw) == 0 || len(raw) > 4096 || !json.Valid(raw) {
		return errors.New("invalid resource request")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("trailing resource request data")
	}
	return nil
}

func validRecordingID(id string) bool { return recordingIDPattern.MatchString(id) }

// RuntimeClient binds all leases to one supervised child identity. Its retry
// loop only repeats a blocked acquisition after a bounded IPC deadline; the
// coordinator's lease operations are idempotent, so an uncertain success is
// safely recovered with the same IDs.
type RuntimeClient struct {
	client  *runtimeipc.Client
	ownerID string
}

func NewRuntimeClient(socketPath string, token []byte, ownerID string) (*RuntimeClient, error) {
	if err := validateIDs(ownerID); err != nil {
		return nil, errors.New("runtime resource owner identity is invalid")
	}
	client, err := runtimeipc.NewClient(socketPath, IPCIdentity, token, ipcTimeout)
	if err != nil {
		return nil, err
	}
	return &RuntimeClient{client: client, ownerID: ownerID}, nil
}

func (c *RuntimeClient) SetReservation(ctx context.Context, recordingID, reservationID string, desiredBytes int64) error {
	if !validRecordingID(recordingID) {
		return errors.New("recording identity is invalid")
	}
	return c.wait(ctx, operationSetReservation, reservationRequest{OwnerID: c.ownerID, RecordingID: recordingID, ReservationID: reservationID, DesiredBytes: desiredBytes})
}

func (c *RuntimeClient) ReleaseReservation(ctx context.Context, recordingID, reservationID string) error {
	if !validRecordingID(recordingID) {
		return errors.New("recording identity is invalid")
	}
	return c.call(ctx, operationReleaseReservation, reservationReleaseRequest{OwnerID: c.ownerID, RecordingID: recordingID, ReservationID: reservationID}, nil)
}

func (c *RuntimeClient) AcquireQueue(ctx context.Context, leaseID string) error {
	return c.wait(ctx, operationAcquireQueue, leaseRequest{OwnerID: c.ownerID, LeaseID: leaseID})
}

func (c *RuntimeClient) ReleaseQueue(ctx context.Context, leaseID string) error {
	return c.call(ctx, operationReleaseQueue, leaseRequest{OwnerID: c.ownerID, LeaseID: leaseID}, nil)
}

func (c *RuntimeClient) AcquireWriter(ctx context.Context, leaseID string) error {
	return c.wait(ctx, operationAcquireWriter, leaseRequest{OwnerID: c.ownerID, LeaseID: leaseID})
}

// ClaimRecording asks the Host to authorize one canonical Recording writer.
// An empty ID lets the Host allocate the Recording ID. The process owner ID is
// bound at RuntimeClient construction and cannot be chosen by the caller.
func (c *RuntimeClient) ClaimRecording(ctx context.Context, recordingID string) (recordingowner.Owner, error) {
	if recordingID != "" && !validRecordingID(recordingID) {
		return recordingowner.Owner{}, errors.New("recording identity is invalid")
	}
	var owner recordingowner.Owner
	if err := c.call(ctx, operationClaimRecording, recordingOwnerClaimRequest{OwnerID: c.ownerID, RecordingID: recordingID}, &owner); err != nil {
		return recordingowner.Owner{}, err
	}
	if !validRecordingID(owner.RecordingID) || owner.EngineGeneration == "" || owner.WorkerInstance == "" || owner.Epoch == 0 || (recordingID != "" && owner.RecordingID != recordingID) {
		return recordingowner.Owner{}, errors.New("recording owner response is invalid")
	}
	return owner, nil
}

// ReleaseRecording releases only the exact Host-issued owner tuple.
func (c *RuntimeClient) ReleaseRecording(ctx context.Context, owner recordingowner.Owner) error {
	if !validRecordingID(owner.RecordingID) || owner.EngineGeneration == "" || owner.WorkerInstance == "" || owner.Epoch == 0 {
		return errors.New("recording owner identity is invalid")
	}
	return c.call(ctx, operationReleaseRecording, recordingOwnerReleaseRequest{OwnerID: c.ownerID, Owner: owner}, nil)
}

func (c *RuntimeClient) ReleaseWriter(ctx context.Context, leaseID string) error {
	return c.call(ctx, operationReleaseWriter, leaseRequest{OwnerID: c.ownerID, LeaseID: leaseID}, nil)
}

func (c *RuntimeClient) Snapshot(ctx context.Context) (Snapshot, error) {
	var snapshot Snapshot
	err := c.call(ctx, operationSnapshot, nil, &snapshot)
	return snapshot, err
}

// ReportTelemetry publishes cumulative local Store counters under this
// RuntimeClient's owner identity. Repeated reports are idempotent.
func (c *RuntimeClient) ReportTelemetry(ctx context.Context, readBytes, writeBytes, errorsTotal uint64) error {
	return c.ReportProcessTelemetry(ctx, readBytes, writeBytes, errorsTotal, 0, 0)
}

// ReportProcessTelemetry publishes cumulative local Store counters and its
// latest queue gauges under this RuntimeClient's bound owner identity.
func (c *RuntimeClient) ReportProcessTelemetry(ctx context.Context, readBytes, writeBytes, errorsTotal uint64, queueBytes int64, oldestAgeSeconds float64) error {
	return c.call(ctx, operationReportTelemetry, telemetryReportRequest{
		OwnerID: c.ownerID, ReadBytes: readBytes, WriteBytes: writeBytes, Errors: errorsTotal,
		QueueBytes: queueBytes, OldestAgeSeconds: oldestAgeSeconds,
	}, nil)
}

// TelemetrySnapshot retrieves the Host-wide bounded storage I/O projection.
func (c *RuntimeClient) TelemetrySnapshot(ctx context.Context) (TelemetrySnapshot, error) {
	var snapshot TelemetrySnapshot
	err := c.call(ctx, operationTelemetrySnapshot, nil, &snapshot)
	return snapshot, err
}

func (c *RuntimeClient) wait(ctx context.Context, operation string, request any) error {
	for {
		err := c.call(ctx, operation, request, nil)
		if err == nil {
			return nil
		}
		var remote *runtimeipc.RemoteError
		if errors.As(err, &remote) && remote.Code == "deadline_exceeded" && (ctx == nil || ctx.Err() == nil) {
			continue
		}
		return err
	}
}

func (c *RuntimeClient) call(ctx context.Context, operation string, request, result any) error {
	if c == nil || c.client == nil {
		return errors.New("runtime resource coordinator is unavailable")
	}
	var payload any
	if request != nil {
		payload = request
	}
	return c.client.Call(ctx, operation, payload, result)
}

var _ interface {
	SetReservation(context.Context, string, string, int64) error
	ReleaseReservation(context.Context, string, string) error
	AcquireQueue(context.Context, string) error
	ReleaseQueue(context.Context, string) error
	AcquireWriter(context.Context, string) error
	ReleaseWriter(context.Context, string) error
	ReportTelemetry(context.Context, uint64, uint64, uint64) error
	ReportProcessTelemetry(context.Context, uint64, uint64, uint64, int64, float64) error
	TelemetrySnapshot(context.Context) (TelemetrySnapshot, error)
	ClaimRecording(context.Context, string) (recordingowner.Owner, error)
	ReleaseRecording(context.Context, recordingowner.Owner) error
} = (*RuntimeClient)(nil)
