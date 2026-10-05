// Command integrated-recorder-adapter-hls is the bundled generic HLS source
// adapter. It speaks Adapter Protocol v1 over stdin/stdout.
package main

import (
	"log"
	"os"

	hlsadapter "github.com/integrated-recorder/core/internal/adapters/hls"
)

func main() {
	log.SetOutput(os.Stderr)
	if err := hlsadapter.Serve(os.Stdin, os.Stdout); err != nil {
		log.Printf("adapter stopped: %v", err)
		os.Exit(1)
	}
}
