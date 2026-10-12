// Package adapterproto defines the language-neutral stdio protocol shared by
// the Core and standalone adapter processes. Frames are one JSON object per
// line; protocol stdout must never contain diagnostic text.
package adapterproto

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const (
	Version       = 1
	MaxFrameBytes = 8 << 20
)

// ErrUnsupportedProtocolVersion identifies a well-formed protocol frame that
// cannot be handled by this implementation. Callers may use errors.Is to
// distinguish compatibility failures from malformed transient output.
var ErrUnsupportedProtocolVersion = errors.New("unsupported protocol version")

// Capabilities name generic protocol behaviors and contain no platform domain.
const (
	CapabilityResolve         = "resolve"
	CapabilityStatus          = "status"
	CapabilityConfigure       = "configure"
	CapabilityInteraction     = "interaction"
	CapabilityMetadata        = "metadata"
	CapabilityEvents          = "events"
	CapabilityRefresh         = "refresh"
	CapabilityResolveWorkflow = "resolve_workflow"
	CapabilityResourceBrowse  = "resource_browse"
	CapabilityWatch           = "watch"
)

const (
	MethodDescribe            = "describe"
	MethodResolve             = "resolve"
	MethodResolveBegin        = "resolve.begin"
	MethodResolveContinue     = "resolve.continue"
	MethodShutdown            = "shutdown"
	MethodGetStatus           = "get_status"
	MethodConfigure           = "configure"
	MethodInteractionBegin    = "interaction.begin"
	MethodInteractionContinue = "interaction.continue"
	MethodMetadata            = "metadata"
	MethodEvents              = "events"
	MethodRefresh             = "refresh"
	MethodResourceList        = "resource.list"
	MethodResourceSearch      = "resource.search"
	MethodWatchCheck          = "watch.check"
)

type Request struct {
	ProtocolVersion int             `json:"protocol_version"`
	ID              string          `json:"id"`
	Method          string          `json:"method"`
	Params          json.RawMessage `json:"params,omitempty"`
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

type Response struct {
	ProtocolVersion int             `json:"protocol_version"`
	ID              string          `json:"id"`
	Result          json.RawMessage `json:"result,omitempty"`
	Error           *Error          `json:"error,omitempty"`
}

type FrameType string

const (
	FrameTypeRequest      FrameType = "request"
	FrameTypeResponse     FrameType = "response"
	FrameTypeNotification FrameType = "notification"
)

// Frame classifies both the original untyped request/response envelopes and
// the reserved typed notification envelope. Current adapter runtime supports
// request/response only; parsing notifications does not deliver them.
type Frame struct {
	Kind         FrameType
	Request      *Request
	Response     *Response
	Notification *Notification
}

// Notification is a reserved v1 extension frame. It deliberately has no ID,
// so it cannot be confused with an ID-bearing response.
type Notification struct {
	ProtocolVersion int             `json:"protocol_version"`
	Method          string          `json:"method"`
	Params          json.RawMessage `json:"params,omitempty"`
}

type notificationEnvelope struct {
	ProtocolVersion int             `json:"protocol_version"`
	Type            FrameType       `json:"type"`
	Method          string          `json:"method"`
	Params          json.RawMessage `json:"params,omitempty"`
}

func Success(id string, result any) (Response, error) {
	data, err := json.Marshal(result)
	if err != nil {
		return Response{}, err
	}
	return Response{ProtocolVersion: Version, ID: id, Result: data}, nil
}

func Failure(id, code, message string, details any) Response {
	return Response{ProtocolVersion: Version, ID: id, Error: &Error{Code: code, Message: message, Details: details}}
}

// ReadRequest reads one bounded JSON frame. A clean EOF between frames is
// returned unchanged; malformed or oversized frames are explicit errors.
func ReadRequest(r *bufio.Reader) (Request, error) {
	line, err := readFrame(r)
	if err != nil {
		return Request{}, err
	}
	frame, err := ParseFrame(line)
	if err != nil {
		return Request{}, err
	}
	if frame.Kind != FrameTypeRequest {
		return Request{}, fmt.Errorf("expected request frame, received %s frame", frame.Kind)
	}
	return *frame.Request, nil
}

// ReadFrame reads one bounded, typed protocol frame. It is used by the Core
// process supervisor to demultiplex async plugin notifications and replies.
func ReadFrame(r *bufio.Reader) (Frame, error) {
	line, err := readFrame(r)
	if err != nil {
		return Frame{}, err
	}
	return ParseFrame(line)
}

func ReadResponse(r *bufio.Reader) (Response, error) {
	frame, err := ReadFrame(r)
	if err != nil {
		return Response{}, err
	}
	if frame.Kind != FrameTypeResponse {
		return Response{}, fmt.Errorf("expected response frame, received %s frame", frame.Kind)
	}
	return *frame.Response, nil
}

func WriteRequest(w io.Writer, request Request) error    { return writeFrame(w, request) }
func WriteResponse(w io.Writer, response Response) error { return writeFrame(w, response) }

func WriteNotification(w io.Writer, notification Notification) error {
	if err := validateNotification(notification); err != nil {
		return err
	}
	return writeFrame(w, notificationEnvelope{ProtocolVersion: notification.ProtocolVersion, Type: FrameTypeNotification, Method: notification.Method, Params: notification.Params})
}

// ParseFrame dispatches a complete JSON envelope. Request/response writers
// keep their legacy wire format; the parser also accepts typed forms and the
// reserved notification extension.
func ParseFrame(data []byte) (Frame, error) {
	if len(data) > MaxFrameBytes {
		return Frame{}, fmt.Errorf("protocol frame exceeds %d bytes", MaxFrameBytes)
	}
	if !utf8.Valid(data) {
		return Frame{}, errors.New("protocol frame is not valid UTF-8")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return Frame{}, fmt.Errorf("malformed protocol frame JSON: %w", err)
	}
	if fields == nil {
		return Frame{}, errors.New("protocol frame must be a JSON object")
	}
	var version int
	if raw, ok := fields["protocol_version"]; !ok || json.Unmarshal(raw, &version) != nil {
		return Frame{}, errors.New("protocol version is required")
	}
	if version != Version {
		return Frame{}, fmt.Errorf("%w %d", ErrUnsupportedProtocolVersion, version)
	}
	if rawType, ok := fields["type"]; ok {
		var frameType string
		if err := json.Unmarshal(rawType, &frameType); err != nil || frameType == "" {
			return Frame{}, errors.New("protocol frame type must be a nonempty string")
		}
		switch FrameType(frameType) {
		case FrameTypeNotification:
			if _, hasID := fields["id"]; hasID {
				return Frame{}, errors.New("notification frame must not contain a request id")
			}
			if _, hasResult := fields["result"]; hasResult {
				return Frame{}, errors.New("notification frame cannot contain a response result")
			}
			if _, hasError := fields["error"]; hasError {
				return Frame{}, errors.New("notification frame cannot contain a response error")
			}
			var notification Notification
			if err := json.Unmarshal(data, &notification); err != nil {
				return Frame{}, fmt.Errorf("malformed notification frame: %w", err)
			}
			if err := validateNotification(notification); err != nil {
				return Frame{}, err
			}
			return Frame{Kind: FrameTypeNotification, Notification: &notification}, nil
		case FrameTypeRequest:
			if _, hasResult := fields["result"]; hasResult {
				return Frame{}, errors.New("request frame cannot contain a response result")
			}
			if _, hasError := fields["error"]; hasError {
				return Frame{}, errors.New("request frame cannot contain a response error")
			}
			return parseRequestFrame(data)
		case FrameTypeResponse:
			if _, hasMethod := fields["method"]; hasMethod {
				return Frame{}, errors.New("response frame cannot contain a method")
			}
			return parseResponseFrame(data)
		default:
			return Frame{}, fmt.Errorf("unknown protocol frame type %q", frameType)
		}
	}

	if _, hasID := fields["id"]; !hasID {
		return Frame{}, errors.New("protocol request/response frame id is required")
	}
	_, hasMethod := fields["method"]
	_, hasResult := fields["result"]
	_, hasError := fields["error"]
	if hasMethod && !hasResult && !hasError {
		return parseRequestFrame(data)
	}
	if !hasMethod && (hasResult || hasError) {
		return parseResponseFrame(data)
	}
	return Frame{}, errors.New("unknown or malformed protocol frame shape")
}

func parseRequestFrame(data []byte) (Frame, error) {
	var request Request
	if err := json.Unmarshal(data, &request); err != nil {
		return Frame{}, fmt.Errorf("malformed request frame: %w", err)
	}
	if strings.TrimSpace(request.ID) == "" || strings.TrimSpace(request.Method) == "" {
		return Frame{}, errors.New("request id and method are required")
	}
	return Frame{Kind: FrameTypeRequest, Request: &request}, nil
}

func parseResponseFrame(data []byte) (Frame, error) {
	var response Response
	if err := json.Unmarshal(data, &response); err != nil {
		return Frame{}, fmt.Errorf("malformed response frame: %w", err)
	}
	if strings.TrimSpace(response.ID) == "" {
		return Frame{}, errors.New("response id is required")
	}
	if response.Error == nil && len(response.Result) == 0 {
		return Frame{}, errors.New("response must contain result or error")
	}
	if response.Error != nil && len(response.Result) != 0 {
		return Frame{}, errors.New("response cannot contain both result and error")
	}
	if response.Error != nil && response.Error.Code == "" {
		return Frame{}, errors.New("structured error code is required")
	}
	return Frame{Kind: FrameTypeResponse, Response: &response}, nil
}

func validateNotification(notification Notification) error {
	if notification.ProtocolVersion != Version {
		return fmt.Errorf("%w %d", ErrUnsupportedProtocolVersion, notification.ProtocolVersion)
	}
	if strings.TrimSpace(notification.Method) == "" {
		return errors.New("notification method is required")
	}
	if len(notification.Params) != 0 && !json.Valid(notification.Params) {
		return errors.New("notification params are invalid JSON")
	}
	return nil
}

func readFrame(r *bufio.Reader) ([]byte, error) {
	var frame []byte
	for {
		chunk, err := r.ReadSlice('\n')
		if len(frame)+len(chunk) > MaxFrameBytes+1 {
			return nil, fmt.Errorf("protocol frame exceeds %d bytes", MaxFrameBytes)
		}
		frame = append(frame, chunk...)
		if err == nil {
			frame = frame[:len(frame)-1]
			if len(frame) == 0 || len(frame) > MaxFrameBytes {
				return nil, errors.New("empty or oversized protocol frame")
			}
			return frame, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) && len(frame) == 0 {
			return nil, io.EOF
		}
		if errors.Is(err, io.EOF) {
			return nil, errors.New("unterminated protocol frame")
		}
		return nil, err
	}
}

func writeFrame(w io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > MaxFrameBytes {
		return fmt.Errorf("protocol frame exceeds %d bytes", MaxFrameBytes)
	}
	data = append(data, '\n')
	for len(data) != 0 {
		n, writeErr := w.Write(data)
		if writeErr != nil {
			return writeErr
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
