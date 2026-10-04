package adapterhost

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/integrated-recorder/core/internal/adapterproto"
)

// TestExternalSDKExampleAcceptance is an opt-in cross-repository acceptance
// test. It deliberately receives only a compiled executable: this Core test
// does not import the SDK or the adapter implementation.
//
// Run it with:
//
//	IR_EXTERNAL_ADAPTER_BINARY=/path/to/integrated-recorder-adapter-example \
//	  go test -race -count=3 ./internal/adapterhost -run '^TestExternalSDKExampleAcceptance$'
func TestExternalSDKExampleAcceptance(t *testing.T) {
	binary := os.Getenv("IR_EXTERNAL_ADAPTER_BINARY")
	if binary == "" {
		t.Skip("set IR_EXTERNAL_ADAPTER_BINARY to a compiled standalone SDK example")
	}
	info, err := os.Stat(binary)
	if err != nil {
		t.Fatalf("stat external adapter binary: %v", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		t.Fatalf("external adapter must be a regular executable file")
	}

	adapterDir := t.TempDir()
	installed := filepath.Join(adapterDir, "integrated-recorder-adapter-example")
	contents, err := os.ReadFile(binary)
	if err != nil {
		t.Fatalf("read external adapter binary: %v", err)
	}
	if err = os.WriteFile(installed, contents, info.Mode().Perm()); err != nil {
		t.Fatalf("install external adapter binary in discovery directory: %v", err)
	}
	if err = os.Chmod(installed, info.Mode().Perm()); err != nil {
		t.Fatalf("make external adapter executable: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	host, err := DiscoverWithState(ctx, adapterDir, nil, nil)
	if err != nil {
		t.Fatalf("Core adapter discovery: %v", err)
	}
	defer host.Close()

	list := host.List()
	if len(list) != 1 || list[0].Descriptor == nil || list[0].Descriptor.ID != "example" {
		t.Fatalf("Core discovery returned %#v, want one `example` adapter", list)
	}
	descriptor, err := host.Descriptor("example")
	if err != nil {
		t.Fatalf("Core descriptor lookup: %v", err)
	}
	if descriptor.ProtocolVersion != 1 {
		t.Fatalf("descriptor protocol version = %d, want 1", descriptor.ProtocolVersion)
	}

	input := json.RawMessage(`{"live":true}`)
	media, err := host.ResolveLegacy(ctx, descriptor.ID, input, nil)
	if err != nil {
		t.Fatalf("Core resolve call: %v", err)
	}
	if err = adapterproto.ValidateMediaSource(media, descriptor.MediaTypes); err != nil {
		t.Fatalf("Core rejected example resolve media: %v", err)
	}

	watch, err := host.WatchCheck(ctx, descriptor.ID, input, nil, nil)
	if err != nil {
		t.Fatalf("Core watch.check call: %v", err)
	}
	if watch.State != "live" || watch.Media == nil {
		t.Fatalf("watch result = %#v, want live result with media", watch)
	}

	metadata, commit, supported, err := host.PrepareMetadata(ctx, descriptor.ID, nil, media)
	if err != nil {
		t.Fatalf("Core metadata call: %v", err)
	}
	if !supported || metadata.Metadata.Title == nil || metadata.Metadata.Description == nil {
		t.Fatalf("metadata result = %#v, supported=%v; want title and description", metadata, supported)
	}
	if err = commit(); err != nil {
		t.Fatalf("commit metadata state mutations: %v", err)
	}

	// Restart goes through Core's process supervisor. The replacement must
	// preserve the descriptor's semantic fingerprint before being accepted.
	restarted, err := host.Restart(ctx, descriptor.ID)
	if err != nil {
		t.Fatalf("Core adapter restart: %v", err)
	}
	if restarted.Descriptor == nil || restarted.Descriptor.ID != descriptor.ID || restarted.Descriptor.Version != descriptor.Version {
		t.Fatalf("restart descriptor = %#v, original = %#v", restarted.Descriptor, descriptor)
	}
	if _, err = host.WatchCheck(ctx, descriptor.ID, input, nil, nil); err != nil {
		t.Fatalf("watch.check after Core-supervised restart: %v", err)
	}
}
