package main

import (
	"context"
	"flag"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/integrated-recorder/core/internal/storageproto"
)

type provider struct{}

func main() {
	socket := flag.String("socket", "", "")
	token := flag.String("token-file", "", "")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := storageproto.Serve(ctx, provider{}, storageproto.ServeOptions{SocketPath: *socket, TokenFile: *token}); err != nil {
		os.Exit(1)
	}
}

func (provider) Descriptor() storageproto.Descriptor {
	return storageproto.Descriptor{
		ProtocolVersion: storageproto.Version,
		ID:              "secret-storage",
		Name:            "Secret Storage",
		Version:         "1.0.0",
		ConfigurationSchema: storageproto.Schema{Fields: []storageproto.Field{
			{Key: "access_token", Control: "secret", Label: "Access token", Required: true},
		}},
		Capabilities: []string{storageproto.CapabilityRead, storageproto.CapabilityWrite, storageproto.CapabilityStat, storageproto.CapabilityList, storageproto.CapabilityDelete, storageproto.CapabilityRangeRead, storageproto.CapabilityAtomicReplace},
	}
}

func (provider) Configure(context.Context, storageproto.Config) error { return nil }
func (provider) Probe(context.Context) error                          { return nil }
func (provider) Put(context.Context, string, io.Reader, int64) (storageproto.ObjectInfo, error) {
	return storageproto.ObjectInfo{}, storageproto.ErrUnsupported
}
func (provider) Open(context.Context, string) (io.ReadCloser, storageproto.ObjectInfo, error) {
	return nil, storageproto.ObjectInfo{}, storageproto.ErrUnsupported
}
func (provider) OpenRange(context.Context, string, int64, int64) (io.ReadCloser, storageproto.ObjectInfo, error) {
	return nil, storageproto.ObjectInfo{}, storageproto.ErrUnsupported
}
func (provider) Stat(context.Context, string) (storageproto.ObjectInfo, error) {
	return storageproto.ObjectInfo{}, storageproto.ErrUnsupported
}
func (provider) List(context.Context, string, string, int) (storageproto.ListPage, error) {
	return storageproto.ListPage{}, storageproto.ErrUnsupported
}
func (provider) Delete(context.Context, string) error { return storageproto.ErrUnsupported }
