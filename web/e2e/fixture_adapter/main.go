package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

func main() {
	if err := serve(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "workflow fixture adapter stopped")
		os.Exit(1)
	}
}

func serve(input io.Reader, output io.Writer) error {
	reader := bufio.NewReaderSize(input, 32<<10)
	workflows := map[string]string{}
	for {
		request, err := adapterproto.ReadRequest(reader)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		var response adapterproto.Response
		switch request.Method {
		case adapterproto.MethodDescribe:
			response, _ = adapterproto.Success(request.ID, describe())
		case adapterproto.MethodResolveBegin:
			var params adapterproto.ResolveBeginParams
			if err = json.Unmarshal(request.Params, &params); err != nil {
				response = adapterproto.Failure(request.ID, "invalid_params", "resolve input is invalid", nil)
				break
			}
			var input struct {
				SourceURL string `json:"source_url"`
			}
			if err = json.Unmarshal(params.Input, &input); err != nil || strings.TrimSpace(input.SourceURL) == "" {
				response = adapterproto.Failure(request.ID, "invalid_input", "resolve input is invalid", nil)
				break
			}
			workflows[params.WorkflowID] = input.SourceURL
			data, _ := json.Marshal(map[string]any{"url": "https://example.test/confirm", "label": "Open fixture action"})
			challenge := &adapterproto.WorkflowChallenge{
				Schema: adapterproto.Schema{Fields: []adapterproto.Field{
					{Key: "answer", Control: "text", Label: "Fixture answer", Required: true},
					{Key: "session_value", Control: "secret", Label: "Fixture secret", Required: true, Persistence: &adapterproto.FieldPersistence{Mode: adapterproto.PersistenceOptional, Target: adapterproto.PersistenceTarget{Scope: adapterproto.PersistencePlugin}}},
				}},
				Prompt: &adapterproto.InteractionMessage{Type: "action", InteractionID: "browser-e2e-action", Title: "Fixture action", Message: "Open this safe external fixture step if needed.", Data: data},
			}
			response, _ = adapterproto.Success(request.ID, adapterproto.ResolveWorkflowResult{State: "configuration_required", WorkflowID: params.WorkflowID, Challenge: challenge})
		case adapterproto.MethodResolveContinue:
			var params adapterproto.ResolveContinueParams
			if err = json.Unmarshal(request.Params, &params); err != nil {
				response = adapterproto.Failure(request.ID, "invalid_params", "workflow input is invalid", nil)
				break
			}
			manifestURL := workflows[params.WorkflowID]
			if manifestURL == "" || strings.TrimSpace(params.AnswerSecrets["session_value"]) == "" || len(params.Answers["answer"]) == 0 {
				response = adapterproto.Failure(request.ID, "invalid_answer", "workflow input is invalid", nil)
				break
			}
			delete(workflows, params.WorkflowID)
			media := &adapterproto.MediaSource{Type: "hls", ManifestURL: manifestURL}
			response, _ = adapterproto.Success(request.ID, adapterproto.ResolveWorkflowResult{State: "resolved", WorkflowID: params.WorkflowID, Media: media})
		case adapterproto.MethodWatchCheck:
			var params adapterproto.WatchCheckParams
			if err = json.Unmarshal(request.Params, &params); err != nil {
				response = adapterproto.Failure(request.ID, "invalid_params", "watch input is invalid", nil)
				break
			}
			var input struct {
				SourceURL string `json:"source_url"`
			}
			if err = json.Unmarshal(params.Input, &input); err != nil || strings.TrimSpace(input.SourceURL) == "" {
				response = adapterproto.Failure(request.ID, "invalid_input", "watch input is invalid", nil)
				break
			}
			base := strings.TrimRight(strings.TrimSpace(input.SourceURL), "/")
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			statusRequest, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/status", nil)
			if requestErr != nil {
				cancel()
				response = adapterproto.Failure(request.ID, "check_failed", "fixture check failed", nil)
				break
			}
			client := &http.Client{Timeout: 3 * time.Second}
			statusResponse, requestErr := client.Do(statusRequest)
			if requestErr != nil {
				cancel()
				response = adapterproto.Failure(request.ID, "check_failed", "fixture check failed", nil)
				break
			}
			data, readErr := io.ReadAll(io.LimitReader(statusResponse.Body, 4097))
			_ = statusResponse.Body.Close()
			cancel()
			if readErr != nil || statusResponse.StatusCode < 200 || statusResponse.StatusCode >= 300 || len(data) > 4096 {
				response = adapterproto.Failure(request.ID, "check_failed", "fixture check failed", nil)
				break
			}
			var status struct {
				Online bool `json:"online"`
			}
			if json.Unmarshal(data, &status) != nil {
				response = adapterproto.Failure(request.ID, "check_failed", "fixture check failed", nil)
				break
			}
			if !status.Online {
				response, _ = adapterproto.Success(request.ID, adapterproto.WatchCheckResult{State: "offline"})
				break
			}
			media := &adapterproto.MediaSource{Type: "hls", ManifestURL: base + "/hls/stream.m3u8"}
			response, _ = adapterproto.Success(request.ID, adapterproto.WatchCheckResult{State: "live", SessionRef: "browser-e2e-session", Media: media})
		case adapterproto.MethodShutdown:
			response, _ = adapterproto.Success(request.ID, map[string]bool{"stopped": true})
			if err = adapterproto.WriteResponse(output, response); err != nil {
				return err
			}
			return nil
		default:
			response = adapterproto.Failure(request.ID, "unsupported_method", "method is not supported", nil)
		}
		if err = adapterproto.WriteResponse(output, response); err != nil {
			return err
		}
	}
}

func describe() adapterproto.Descriptor {
	return adapterproto.Descriptor{
		ID: "workflow-fixture", Name: "Workflow Fixture", Version: "0.1.0", ProtocolVersion: adapterproto.Version,
		Capabilities:        []string{adapterproto.CapabilityResolveWorkflow, adapterproto.CapabilityWatch},
		InputSchema:         adapterproto.Schema{Fields: []adapterproto.Field{{Key: "source_url", Control: "text", Label: "Fixture source URL", Required: true}}},
		ConfigurationSchema: adapterproto.Schema{Fields: []adapterproto.Field{}},
		ResourceTypes:       []adapterproto.ResourceType{}, MediaTypes: []string{"hls"},
	}
}
