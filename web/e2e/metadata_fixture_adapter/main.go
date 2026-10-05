// Command metadata-fixture-adapter is an E2E-only Protocol v1 adapter. It
// exercises Core metadata polling with a mutable local fixture, without
// weakening production public-network checks.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

const maxMetadataResponseBytes = 64 << 10

func main() {
	if err := serve(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "metadata fixture adapter stopped")
		os.Exit(1)
	}
}

func serve(input io.Reader, output io.Writer) error {
	reader := bufio.NewReaderSize(input, 32<<10)
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
		case adapterproto.MethodResolve:
			var params adapterproto.ResolveParams
			var input struct {
				SourceURL string `json:"source_url"`
			}
			if json.Unmarshal(request.Params, &params) != nil || json.Unmarshal(params.Input, &input) != nil {
				response = adapterproto.Failure(request.ID, "invalid_input", "fixture input is invalid", nil)
				break
			}
			origin, err := fixtureOrigin(input.SourceURL)
			if err != nil {
				response = adapterproto.Failure(request.ID, "invalid_input", "fixture input is invalid", nil)
				break
			}
			response, _ = adapterproto.Success(request.ID, adapterproto.MediaSource{Type: "hls", ManifestURL: origin + "/hls/stream.m3u8"})
		case adapterproto.MethodMetadata:
			var params adapterproto.MetadataParams
			if json.Unmarshal(request.Params, &params) != nil {
				response = adapterproto.Failure(request.ID, "invalid_params", "metadata request is invalid", nil)
				break
			}
			origin, err := fixtureOrigin(params.Current.ManifestURL)
			if err != nil {
				response = adapterproto.Failure(request.ID, "metadata_failed", "metadata observation failed", nil)
				break
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			metadataRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/e2e/metadata", nil)
			if err != nil {
				cancel()
				response = adapterproto.Failure(request.ID, "metadata_failed", "metadata observation failed", nil)
				break
			}
			metadataResponse, err := (&http.Client{Timeout: 3 * time.Second}).Do(metadataRequest)
			if err != nil {
				cancel()
				response = adapterproto.Failure(request.ID, "metadata_failed", "metadata observation failed", nil)
				break
			}
			data, readErr := io.ReadAll(io.LimitReader(metadataResponse.Body, maxMetadataResponseBytes+1))
			_ = metadataResponse.Body.Close()
			cancel()
			if readErr != nil || metadataResponse.StatusCode < 200 || metadataResponse.StatusCode >= 300 || len(data) > maxMetadataResponseBytes {
				response = adapterproto.Failure(request.ID, "metadata_failed", "metadata observation failed", nil)
				break
			}
			var source struct {
				Title       *string `json:"title"`
				Description *string `json:"description"`
			}
			if json.Unmarshal(data, &source) != nil {
				response = adapterproto.Failure(request.ID, "metadata_failed", "metadata observation failed", nil)
				break
			}
			result := adapterproto.MetadataResult{Metadata: adapterproto.StreamMetadata{Title: source.Title, Description: source.Description}}
			if result.Validate() != nil {
				response = adapterproto.Failure(request.ID, "metadata_failed", "metadata observation failed", nil)
				break
			}
			response, _ = adapterproto.Success(request.ID, result)
		case adapterproto.MethodShutdown:
			response, _ = adapterproto.Success(request.ID, map[string]bool{"stopped": true})
			if err = adapterproto.WriteResponse(output, response); err != nil {
				return err
			}
			return nil
		default:
			response = adapterproto.Failure(request.ID, "unsupported_method", "fixture method is unsupported", nil)
		}
		if err = adapterproto.WriteResponse(output, response); err != nil {
			return err
		}
	}
}

func describe() adapterproto.Descriptor {
	return adapterproto.Descriptor{
		ID: "metadata-fixture", Name: "Metadata Fixture", Version: "0.1.0", ProtocolVersion: adapterproto.Version,
		Capabilities:        []string{adapterproto.CapabilityResolve, adapterproto.CapabilityMetadata},
		InputSchema:         adapterproto.Schema{Fields: []adapterproto.Field{{Key: "source_url", Control: "text", Label: "Fixture source URL", Required: true}}},
		ConfigurationSchema: adapterproto.Schema{Fields: []adapterproto.Field{}},
		ResourceTypes:       []adapterproto.ResourceType{}, MediaTypes: []string{"hls"},
	}
}

func fixtureOrigin(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme != "http" || u.Hostname() != "127.0.0.1" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("invalid fixture source")
	}
	return u.Scheme + "://" + u.Host, nil
}
