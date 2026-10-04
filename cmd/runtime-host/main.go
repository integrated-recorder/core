// Command runtime-host is PID 1 in the production image. It owns the stable
// external listener and supervises separate Control and Recorder Engine
// processes from the immutable image-bundled application release.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/integrated-recorder/core/internal/runtimehost/bootstrap"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "setup-code" {
		dataDir := os.Getenv("DATA_DIR")
		if dataDir == "" {
			dataDir = "/data"
		}
		if err := bootstrap.PrintSetupCode(dataDir, os.Stdout); err != nil {
			log.Printf("runtime-host: setup code is unavailable")
			os.Exit(1)
		}
		return
	}
	if len(os.Args) != 1 {
		log.Printf("runtime-host: supported command: setup-code")
		os.Exit(2)
	}
	config, err := bootstrap.ConfigFromEnv(os.Getenv)
	if err != nil {
		log.Printf("runtime-host: invalid startup configuration: %v", err)
		os.Exit(1)
	}
	if err := configureRuntimeE2EPluginRegistryTrust(&config); err != nil {
		log.Printf("runtime-host: test registry transport configuration is invalid")
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := bootstrap.Run(ctx, config); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("runtime-host: %v", err)
		os.Exit(1)
	}
}
