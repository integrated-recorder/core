package main

import (
	"log"
	"os"

	"github.com/integrated-recorder/core/internal/adapters/owncast"
)

func main() {
	if err := owncast.Serve(os.Stdin, os.Stdout); err != nil {
		log.Printf("adapter stopped: %v", err)
		os.Exit(1)
	}
}
