// Command timed-event-fixture is a deterministic E2E-only source plugin. It
// emits generic timed events through the v1 events capability and uses only
// the loopback media fixture configured by the browser test.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

type eventStream struct {
	streamID string
	phase    int
	next     int
	waiting  string
}

type fixtureAdapter struct {
	starts  int
	streams map[string]*eventStream
}

func main() {
	adapter := &fixtureAdapter{streams: make(map[string]*eventStream)}
	if err := adapter.serve(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "timed event fixture adapter stopped")
		os.Exit(1)
	}
}

func (a *fixtureAdapter) serve(input io.Reader, output io.Writer) error {
	reader := bufio.NewReaderSize(input, 32<<10)
	for {
		request, err := adapterproto.ReadRequest(reader)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		var response adapterproto.Response
		switch request.Method {
		case adapterproto.MethodDescribe:
			response, _ = adapterproto.Success(request.ID, a.describe())
		case adapterproto.MethodResolve:
			var params adapterproto.ResolveParams
			var source struct {
				SourceURL string `json:"source_url"`
			}
			if json.Unmarshal(request.Params, &params) != nil || json.Unmarshal(params.Input, &source) != nil {
				response = adapterproto.Failure(request.ID, "invalid_input", "fixture input is invalid", nil)
				break
			}
			origin, originErr := fixtureOrigin(source.SourceURL)
			if originErr != nil {
				response = adapterproto.Failure(request.ID, "invalid_input", "fixture input is invalid", nil)
				break
			}
			response, _ = adapterproto.Success(request.ID, adapterproto.MediaSource{Type: "hls", ManifestURL: origin + "/hls/stream.m3u8"})
		case adapterproto.MethodEventsStart:
			var params adapterproto.TimedEventStartParams
			if json.Unmarshal(request.Params, &params) != nil || params.StreamID == "" {
				response = adapterproto.Failure(request.ID, "invalid_params", "event stream parameters are invalid", nil)
				break
			}
			phase, ok := fixturePhase(params.ResumeState)
			if !ok {
				response = adapterproto.Failure(request.ID, "invalid_resume_state", "fixture resume state is invalid", nil)
				break
			}
			a.starts++
			a.streams[params.StreamID] = &eventStream{streamID: params.StreamID, phase: phase}
			response, _ = adapterproto.Success(request.ID, adapterproto.TimedEventStartResult{Accepted: true})
		case adapterproto.MethodEventsAck:
			var params adapterproto.TimedEventAckParams
			if json.Unmarshal(request.Params, &params) != nil {
				response = adapterproto.Failure(request.ID, "invalid_ack", "event acknowledgement is invalid", nil)
				break
			}
			stream := a.streams[params.StreamID]
			if stream == nil || stream.waiting == "" || params.BatchID != stream.waiting {
				response = adapterproto.Failure(request.ID, "invalid_ack", "event acknowledgement is invalid", nil)
				break
			}
			stream.waiting = ""
			stream.next++
			response, _ = adapterproto.Success(request.ID, map[string]bool{"acknowledged": true})
		case adapterproto.MethodEventsStop:
			var params adapterproto.TimedEventStopParams
			if json.Unmarshal(request.Params, &params) == nil {
				delete(a.streams, params.StreamID)
			}
			response, _ = adapterproto.Success(request.ID, map[string]bool{"stopped": true})
		case adapterproto.MethodShutdown:
			response, _ = adapterproto.Success(request.ID, map[string]bool{"stopped": true})
			if err := adapterproto.WriteResponse(output, response); err != nil {
				return err
			}
			return nil
		default:
			response = adapterproto.Failure(request.ID, "unsupported_method", "fixture method is unsupported", nil)
		}
		if err := adapterproto.WriteResponse(output, response); err != nil {
			return err
		}
		if request.Method == adapterproto.MethodEventsStart {
			var params adapterproto.TimedEventStartParams
			if json.Unmarshal(request.Params, &params) == nil {
				if err := a.emitNext(output, params.StreamID); err != nil {
					return err
				}
			}
		}
		if request.Method == adapterproto.MethodEventsAck {
			var params adapterproto.TimedEventAckParams
			if json.Unmarshal(request.Params, &params) == nil {
				if err := a.emitNext(output, params.StreamID); err != nil {
					return err
				}
			}
		}
	}
}

func (a *fixtureAdapter) describe() adapterproto.Descriptor {
	return adapterproto.Descriptor{
		ID: "timed-event-fixture", Name: "Timed Event Fixture", Version: "0.1.0", ProtocolVersion: adapterproto.Version,
		Capabilities:        []string{adapterproto.CapabilityResolve, adapterproto.CapabilityEvents},
		InputSchema:         adapterproto.Schema{Fields: []adapterproto.Field{{Key: "source_url", Control: "text", Label: "Fixture source URL", Required: true}}},
		ConfigurationSchema: adapterproto.Schema{Fields: []adapterproto.Field{}}, ResourceTypes: []adapterproto.ResourceType{}, MediaTypes: []string{"hls"},
	}
}

func (a *fixtureAdapter) emitNext(output io.Writer, streamID string) error {
	stream := a.streams[streamID]
	if stream == nil || stream.waiting != "" {
		return nil
	}
	var batch adapterproto.TimedEventBatch
	switch {
	case stream.phase == 0 && stream.next == 0:
		stamp := time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)
		sequence := uint64(42)
		batch = adapterproto.TimedEventBatch{StreamID: streamID, BatchID: "fixture-batch-1", ResumeState: json.RawMessage(`{"cursor":"fixture-1"}`), Events: []adapterproto.TimedEvent{
			{EventType: "chat.message", SourceEventID: "fixture-message-1", SourceTimestamp: &stamp, Actor: &adapterproto.TimedEventActor{DisplayName: "viewer"}, Message: &adapterproto.TimedEventMessage{Text: "fixture chat message"}},
			{EventType: "chat.message", SourceTimestamp: &stamp, Actor: &adapterproto.TimedEventActor{DisplayName: "anonymous"}, Message: &adapterproto.TimedEventMessage{Text: "source-less chat message"}},
			{EventType: "custom.example", Extension: json.RawMessage(`{"label":"custom fixture event"}`)},
			{EventType: "source.sequence", SourceSequence: &sequence, Message: &adapterproto.TimedEventMessage{Text: "sequence fixture event"}},
		}}
	case stream.phase == 0 && stream.next == 1:
		stamp := time.Date(2026, 10, 11, 9, 59, 0, 0, time.UTC)
		batch = adapterproto.TimedEventBatch{StreamID: streamID, BatchID: "fixture-batch-2", ResumeState: json.RawMessage(`{"cursor":"fixture-2"}`), Events: []adapterproto.TimedEvent{
			{EventType: "chat.edit", RelatedEventID: "fixture-message-1", Message: &adapterproto.TimedEventMessage{Text: "edited fixture message"}},
			{EventType: "chat.delete", Actor: &adapterproto.TimedEventActor{DisplayName: "moderator"}},
			{EventType: "chat.system", SourceTimestamp: &stamp, Message: &adapterproto.TimedEventMessage{Text: "fixture system event"}},
			{EventType: "support.donation", Actor: &adapterproto.TimedEventActor{DisplayName: "supporter"}, Amount: "5.00", Currency: "USD"},
			{EventType: "support.subscription", Actor: &adapterproto.TimedEventActor{DisplayName: "subscriber"}},
		}}
	case stream.phase == 0 && stream.next == 2:
		params := map[string]string{"stream_id": streamID, "reason": "completed"}
		encoded, _ := json.Marshal(params)
		return adapterproto.WriteNotification(output, adapterproto.Notification{ProtocolVersion: adapterproto.Version, Method: adapterproto.MethodEventsEnded, Params: encoded})
	case stream.phase == 1 && stream.next == 0:
		stamp := time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)
		sequence := uint64(42)
		batch = adapterproto.TimedEventBatch{StreamID: streamID, BatchID: "fixture-reconnect-replay", ResumeState: json.RawMessage(`{"cursor":"fixture-3"}`), Events: []adapterproto.TimedEvent{
			{EventType: "chat.message", SourceEventID: "fixture-message-1", SourceTimestamp: &stamp, Actor: &adapterproto.TimedEventActor{DisplayName: "viewer"}, Message: &adapterproto.TimedEventMessage{Text: "fixture chat message"}},
			{EventType: "source.sequence", SourceSequence: &sequence, Message: &adapterproto.TimedEventMessage{Text: "sequence fixture event"}},
			{EventType: "chat.message", SourceEventID: "fixture-message-2", Actor: &adapterproto.TimedEventActor{DisplayName: "viewer-2"}, Message: &adapterproto.TimedEventMessage{Text: "after reconnect"}},
		}}
	case stream.phase == 2 && stream.next == 0:
		batch = adapterproto.TimedEventBatch{StreamID: streamID, BatchID: "fixture-process-restart", ResumeState: json.RawMessage(`{"cursor":"fixture-4"}`), Events: []adapterproto.TimedEvent{
			{EventType: "chat.message", SourceEventID: "fixture-message-3", Actor: &adapterproto.TimedEventActor{DisplayName: "viewer-3"}, Message: &adapterproto.TimedEventMessage{Text: "after adapter process restart"}},
		}}
	default:
		return nil
	}
	if err := batch.Validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	stream.waiting = batch.BatchID
	return adapterproto.WriteNotification(output, adapterproto.Notification{ProtocolVersion: adapterproto.Version, Method: adapterproto.MethodEventsBatch, Params: encoded})
}

func fixturePhase(raw json.RawMessage) (int, bool) {
	if len(raw) == 0 {
		return 0, true
	}
	var state struct {
		Cursor string `json:"cursor"`
	}
	if json.Unmarshal(raw, &state) != nil {
		return 0, false
	}
	switch state.Cursor {
	case "fixture-2":
		return 1, true
	case "fixture-3":
		return 2, true
	case "fixture-4":
		return 3, true
	default:
		return 0, false
	}
}

func fixtureOrigin(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("fixture source is invalid")
	}
	return u.Scheme + "://" + u.Host, nil
}
