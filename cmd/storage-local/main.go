package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/integrated-recorder/core/internal/storagelocal"
	"github.com/integrated-recorder/core/internal/storageproto"
)

func main() {
	socket := flag.String("socket", "", "absolute path to the private provider Unix socket")
	tokenFile := flag.String("token-file", "", "absolute path to the private provider token file")
	flag.Parse()
	if *socket == "" || *tokenFile == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "storage-local requires --socket and --token-file")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := storageproto.Serve(ctx, storagelocal.New(), storageproto.ServeOptions{
		SocketPath:         *socket,
		TokenFile:          *tokenFile,
		MaxConcurrentReads: storageproto.MaxConcurrentReadStreams,
	}); err != nil {
		fmt.Fprintln(os.Stderr, "storage-local stopped:", err)
		os.Exit(1)
	}
}
