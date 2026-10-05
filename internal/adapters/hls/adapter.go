// Package hls implements the bundled, generic HLS manifest source adapter.
// It validates and returns direct manifest URLs; media acquisition remains
// owned by the Core.
package hls

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/integrated-recorder/core/internal/adapterproto"
	"github.com/integrated-recorder/core/internal/buildinfo"
)

const MaxManifestURLBytes = 8 << 10

// Describe returns the protocol identity for this Core release's bundled HLS
// adapter. Trust and publisher provenance are assigned by Runtime Host, never
// by the executable descriptor.
func Describe() adapterproto.Descriptor {
	return adapterproto.Descriptor{
		ID:              "hls",
		Name:            "HLS",
		Version:         buildinfo.Current().Version,
		ProtocolVersion: adapterproto.Version,
		Capabilities:    []string{adapterproto.CapabilityResolve},
		InputSchema: adapterproto.Schema{Fields: []adapterproto.Field{{
			Key:      "manifest_url",
			Control:  "text",
			Label:    "HLS manifest URL",
			Required: true,
			Constraints: &adapterproto.Constraints{
				MaxLength: intPointer(MaxManifestURLBytes),
			},
		}}},
		ConfigurationSchema: adapterproto.Schema{Fields: []adapterproto.Field{}},
		ResourceTypes:       []adapterproto.ResourceType{},
		MediaTypes:          []string{"hls"},
	}
}

func intPointer(value int) *int { return &value }

// Resolve accepts a direct HTTP(S) manifest URL without performing network
// access. Query parameters are preserved and fragments are removed because
// they are not sent in HTTP requests.
func Resolve(input json.RawMessage) (adapterproto.MediaSource, error) {
	if len(input) == 0 || !utf8.Valid(input) {
		return adapterproto.MediaSource{}, errors.New("manifest URL input is invalid")
	}
	if err := adapterproto.ValidateObjectAgainstSchema(Describe().InputSchema, input); err != nil {
		return adapterproto.MediaSource{}, errors.New("manifest URL input is invalid")
	}
	var values struct {
		ManifestURL string `json:"manifest_url"`
	}
	if err := json.Unmarshal(input, &values); err != nil {
		return adapterproto.MediaSource{}, errors.New("manifest URL input is invalid")
	}
	raw := values.ManifestURL
	if raw == "" || len(raw) > MaxManifestURLBytes || !utf8.ValidString(raw) || strings.TrimSpace(raw) != raw {
		return adapterproto.MediaSource{}, errors.New("manifest URL is invalid")
	}
	for _, r := range raw {
		if unicode.IsControl(r) {
			return adapterproto.MediaSource{}, errors.New("manifest URL is invalid")
		}
	}

	u, err := url.Parse(raw)
	if err != nil || u.Opaque != "" || u.User != nil {
		return adapterproto.MediaSource{}, errors.New("manifest URL is invalid")
	}
	u.Scheme = strings.ToLower(u.Scheme)
	if (u.Scheme != "http" && u.Scheme != "https") || !u.IsAbs() || u.Host == "" || u.Hostname() == "" {
		return adapterproto.MediaSource{}, errors.New("manifest URL must be an HTTP or HTTPS URL with a host")
	}
	u.Fragment = ""
	u.RawFragment = ""
	manifestURL := u.String()
	if len(manifestURL) > MaxManifestURLBytes {
		return adapterproto.MediaSource{}, errors.New("manifest URL is too long")
	}
	media := adapterproto.MediaSource{Type: "hls", ManifestURL: manifestURL}
	if err := adapterproto.ValidateMediaSource(media, []string{"hls"}); err != nil {
		return adapterproto.MediaSource{}, errors.New("manifest URL is invalid")
	}
	return media, nil
}

// Serve implements the sequential Adapter Protocol v1 stdin/stdout contract.
// stdout is used only for protocol frames; callers should send diagnostics to
// stderr.
func Serve(input io.Reader, output io.Writer) error {
	reader := bufio.NewReaderSize(input, 32<<10)
	for {
		request, err := adapterproto.ReadRequest(reader)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			id := request.ID
			if id == "" {
				id = "0"
			}
			code := "malformed_request"
			if errors.Is(err, adapterproto.ErrUnsupportedProtocolVersion) {
				code = "unsupported_protocol_version"
			}
			_ = adapterproto.WriteResponse(output, adapterproto.Failure(id, code, "invalid adapter request", nil))
			if code == "malformed_request" {
				return nil
			}
			continue
		}

		var response adapterproto.Response
		switch request.Method {
		case adapterproto.MethodDescribe:
			response, err = adapterproto.Success(request.ID, Describe())
		case adapterproto.MethodResolve:
			var params adapterproto.ResolveParams
			if len(request.Params) == 0 || json.Unmarshal(request.Params, &params) != nil {
				response = adapterproto.Failure(request.ID, "invalid_params", "resolve params are invalid", nil)
			} else {
				media, resolveErr := Resolve(params.Input)
				if resolveErr != nil {
					response = adapterproto.Failure(request.ID, "invalid_input", "manifest URL input is invalid", nil)
				} else {
					response, err = adapterproto.Success(request.ID, media)
				}
			}
		case adapterproto.MethodShutdown:
			response, err = adapterproto.Success(request.ID, map[string]bool{"stopped": true})
		default:
			response = adapterproto.Failure(request.ID, "unsupported_method", "method is not supported", nil)
		}
		if err != nil {
			return fmt.Errorf("encode adapter response: %w", err)
		}
		if err := adapterproto.WriteResponse(output, response); err != nil {
			return err
		}
		if request.Method == adapterproto.MethodShutdown {
			return nil
		}
	}
}
