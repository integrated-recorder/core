// Command integrated-recorder-adapter-runtime-update-fixture is a Protocol v1
// external fixture adapter used only by the signed Runtime Host acceptance test.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

const maxFixtureResponseBytes = 64 << 10

// These linker-overridable fields let the Runtime Host process E2E build
// multiple immutable adapter artifacts from the same deterministic fixture.
var (
	fixtureAdapterID      = "runtime-update-fixture"
	fixtureAdapterVersion = "0.1.0"
)

func main() {
	if err := serve(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "runtime update fixture adapter stopped")
		os.Exit(1)
	}
}

func serve(input io.Reader, output io.Writer) error {
	reader := bufio.NewReaderSize(input, 32<<10)
	client := &http.Client{Timeout: 12 * time.Second}
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
			response, _ = adapterproto.Success(request.ID, descriptor())
		case adapterproto.MethodResolve:
			var params adapterproto.ResolveParams
			var input struct {
				SourceURL string `json:"source_url"`
			}
			if json.Unmarshal(request.Params, &params) != nil || json.Unmarshal(params.Input, &input) != nil {
				response = adapterproto.Failure(request.ID, "invalid_input", "fixture input is invalid", nil)
				break
			}
			media, err := resolveSource(input.SourceURL)
			if err != nil {
				response = adapterproto.Failure(request.ID, "resolve_failed", "fixture source could not be resolved", nil)
				break
			}
			response, _ = adapterproto.Success(request.ID, media)
		case adapterproto.MethodWatchCheck:
			var params adapterproto.WatchCheckParams
			var input struct {
				SourceURL string `json:"source_url"`
			}
			if json.Unmarshal(request.Params, &params) != nil || json.Unmarshal(params.Input, &input) != nil {
				response = adapterproto.Failure(request.ID, "invalid_input", "fixture input is invalid", nil)
				break
			}
			origin, stream, err := sourceIdentity(input.SourceURL)
			if err != nil {
				response = adapterproto.Failure(request.ID, "watch_failed", "fixture source could not be checked", nil)
				break
			}
			var state fixtureState
			if err := getJSON(client, origin+"/e2e/state?stream="+url.QueryEscape(stream), &state); err != nil {
				response = adapterproto.Failure(request.ID, "watch_failed", "fixture source could not be checked", nil)
				break
			}
			result := adapterproto.WatchCheckResult{State: "offline", Title: state.Title}
			if state.Online {
				media := mediaFor(origin, stream, "token-0", state.SessionID)
				result.State, result.SessionRef, result.Media = "live", state.SessionID, &media
			}
			response, _ = adapterproto.Success(request.ID, result)
		case adapterproto.MethodMetadata:
			var params adapterproto.MetadataParams
			if json.Unmarshal(request.Params, &params) != nil {
				response = adapterproto.Failure(request.ID, "invalid_params", "metadata request is invalid", nil)
				break
			}
			origin, stream, err := mediaIdentity(params.Current.ManifestURL)
			if err != nil {
				response = adapterproto.Failure(request.ID, "metadata_failed", "metadata observation failed", nil)
				break
			}
			var metadata struct {
				Title       *string `json:"title"`
				Description *string `json:"description"`
			}
			if err := getJSON(client, origin+"/e2e/metadata?stream="+url.QueryEscape(stream), &metadata); err != nil {
				response = adapterproto.Failure(request.ID, "metadata_failed", "metadata observation failed", nil)
				break
			}
			result := adapterproto.MetadataResult{Metadata: adapterproto.StreamMetadata{Title: metadata.Title, Description: metadata.Description}}
			if result.Validate() != nil {
				response = adapterproto.Failure(request.ID, "metadata_failed", "metadata observation failed", nil)
				break
			}
			response, _ = adapterproto.Success(request.ID, result)
		case adapterproto.MethodRefresh:
			var params adapterproto.RefreshParams
			if json.Unmarshal(request.Params, &params) != nil {
				response = adapterproto.Failure(request.ID, "invalid_params", "refresh request is invalid", nil)
				break
			}
			origin, stream, token, err := mediaParts(params.Current.ManifestURL)
			if err != nil {
				response = adapterproto.Failure(request.ID, "refresh_failed", "fixture source could not be refreshed", nil)
				break
			}
			var refreshed struct {
				Token string `json:"token"`
			}
			endpoint := origin + "/e2e/refresh?stream=" + url.QueryEscape(stream) + "&token=" + url.QueryEscape(token)
			if err := getJSON(client, endpoint, &refreshed); err != nil || refreshed.Token == "" {
				response = adapterproto.Failure(request.ID, "refresh_failed", "fixture source could not be refreshed", nil)
				break
			}
			response, _ = adapterproto.Success(request.ID, adapterproto.RefreshResult{Media: mediaFor(origin, stream, refreshed.Token, "")})
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
	}
}

type fixtureState struct {
	Online    bool   `json:"online"`
	SessionID string `json:"session_id"`
	Title     string `json:"title"`
}

func descriptor() adapterproto.Descriptor {
	return adapterproto.Descriptor{
		ID: fixtureAdapterID, Name: "Runtime Update Fixture", Version: fixtureAdapterVersion, ProtocolVersion: adapterproto.Version,
		Capabilities:        []string{adapterproto.CapabilityResolve, adapterproto.CapabilityWatch, adapterproto.CapabilityMetadata, adapterproto.CapabilityRefresh},
		InputSchema:         adapterproto.Schema{Fields: []adapterproto.Field{{Key: "source_url", Control: "text", Label: "Fixture source URL", Required: true}}},
		ConfigurationSchema: adapterproto.Schema{Fields: []adapterproto.Field{}}, ResourceTypes: []adapterproto.ResourceType{}, MediaTypes: []string{"hls"},
	}
}

func resolveSource(raw string) (adapterproto.MediaSource, error) {
	origin, stream, err := sourceIdentity(raw)
	if err != nil {
		return adapterproto.MediaSource{}, err
	}
	return mediaFor(origin, stream, "token-0", ""), nil
}

func mediaFor(origin, stream, token, session string) adapterproto.MediaSource {
	manifest := origin + "/hls/stream.m3u8?stream=" + url.QueryEscape(stream) + "&token=" + url.QueryEscape(token)
	return adapterproto.MediaSource{
		Type: "hls", ManifestURL: manifest, SessionRef: session,
		RefreshPolicy: &adapterproto.RefreshPolicy{OnHTTPStatus: []int{http.StatusForbidden}},
	}
}

func sourceIdentity(raw string) (string, string, error) {
	u, err := url.ParseRequestURI(strings.TrimSpace(raw))
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.User != nil || u.Fragment != "" {
		return "", "", errors.New("fixture source URL is invalid")
	}
	stream := strings.TrimPrefix(u.Path, "/source/")
	if stream == "" || strings.Contains(stream, "/") {
		return "", "", errors.New("fixture stream identity is invalid")
	}
	return "http://" + u.Host, stream, nil
}

func mediaIdentity(raw string) (string, string, error) {
	origin, stream, _, err := mediaParts(raw)
	return origin, stream, err
}

func mediaParts(raw string) (string, string, string, error) {
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.User != nil || u.Fragment != "" {
		return "", "", "", errors.New("fixture media URL is invalid")
	}
	stream, token := u.Query().Get("stream"), u.Query().Get("token")
	if stream == "" || token == "" || strings.Contains(stream, "/") {
		return "", "", "", errors.New("fixture media identity is invalid")
	}
	return "http://" + u.Host, stream, token, nil
}

func getJSON(client *http.Client, endpoint string, target any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return errors.New("fixture request is invalid")
	}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("fixture request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return errors.New("fixture response was not successful")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxFixtureResponseBytes+1))
	if err != nil || len(data) > maxFixtureResponseBytes {
		return errors.New("fixture response exceeded its bound")
	}
	if err := json.Unmarshal(data, target); err != nil {
		return errors.New("fixture response was malformed")
	}
	return nil
}
