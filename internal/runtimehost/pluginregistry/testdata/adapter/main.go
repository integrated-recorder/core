// Command adapter is a deterministic process fixture for Plugin Registry tests.
// It intentionally exercises the same stdio Protocol v1 probe as real adapters.
package main

import (
	"bufio"
	"errors"
	"io"
	"os"
	"strconv"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

var (
	fixtureID       = "registry-fixture"
	fixtureVersion  = "1.0.0"
	fixtureProtocol = "1"
)

func main() {
	protocol, _ := strconv.Atoi(fixtureProtocol)
	reader := bufio.NewReader(os.Stdin)
	for {
		request, err := adapterproto.ReadRequest(reader)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			os.Exit(2)
		}
		if request.Method == adapterproto.MethodShutdown {
			response, _ := adapterproto.Success(request.ID, map[string]any{})
			_ = adapterproto.WriteResponse(os.Stdout, response)
			return
		}
		if request.Method != adapterproto.MethodDescribe {
			_ = adapterproto.WriteResponse(os.Stdout, adapterproto.Failure(request.ID, "unsupported_method", "unsupported", nil))
			continue
		}
		descriptor := adapterproto.Descriptor{
			ID: fixtureID, Name: "Registry Test Fixture", Version: fixtureVersion,
			ProtocolVersion: protocol, Capabilities: []string{adapterproto.CapabilityResolve},
			InputSchema:         adapterproto.Schema{Fields: []adapterproto.Field{}},
			ConfigurationSchema: adapterproto.Schema{Fields: []adapterproto.Field{}},
			MediaTypes:          []string{"application/vnd.integrated-recorder.test"},
		}
		response, err := adapterproto.Success(request.ID, descriptor)
		if err != nil || adapterproto.WriteResponse(os.Stdout, response) != nil {
			os.Exit(3)
		}
	}
}
