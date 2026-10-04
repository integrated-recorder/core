package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/integrated-recorder/core/internal/storageproto"
)

func main() {
	binary := flag.String("binary", "", "absolute path to storage provider executable")
	configPath := flag.String("config", "", "optional private JSON provider configuration")
	jsonOutput := flag.Bool("json", false, "write machine-readable JSON report")
	flag.Parse()
	if *binary == "" || flag.NArg() != 0 {
		fail(*jsonOutput, storageproto.ConformanceReport{ProtocolVersion: storageproto.Version, Checks: []storageproto.ConformanceCheck{{Name: "arguments", Pass: false}}})
		return
	}
	var config *storageproto.Config
	if *configPath != "" {
		loaded, err := readConfig(*configPath)
		if err != nil {
			fail(*jsonOutput, storageproto.ConformanceReport{ProtocolVersion: storageproto.Version, Checks: []storageproto.ConformanceCheck{{Name: "config", Pass: false}}})
			return
		}
		config = &loaded
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	report, err := storageproto.RunConformance(ctx, *binary, config)
	if err != nil {
		var conformance *storageproto.ConformanceError
		check := "conformance"
		if errors.As(err, &conformance) {
			check = conformance.Check
		}
		report.Passed = false
		report.Checks = append(report.Checks, storageproto.ConformanceCheck{Name: check, Pass: false})
		fail(*jsonOutput, report)
		return
	}
	if *jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetEscapeHTML(true)
		if encoder.Encode(report) != nil {
			os.Exit(1)
		}
		return
	}
	for _, check := range report.Checks {
		if check.Pass {
			fmt.Printf("PASS %s\n", check.Name)
		} else {
			fmt.Printf("FAIL %s\n", check.Name)
		}
	}
	fmt.Printf("Storage Provider Protocol v%d: %s %s\n", report.ProtocolVersion, report.ProviderID, report.ProviderVersion)
}

func readConfig(path string) (storageproto.Config, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 0 || info.Size() > storageproto.MaxConfigBytes || info.Mode().Perm()&0077 != 0 {
		return storageproto.Config{}, errors.New("invalid config file")
	}
	f, err := os.Open(path)
	if err != nil {
		return storageproto.Config{}, errors.New("invalid config file")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, storageproto.MaxConfigBytes+1))
	if err != nil || len(data) > storageproto.MaxConfigBytes {
		return storageproto.Config{}, errors.New("invalid config file")
	}
	defer func() {
		for i := range data {
			data[i] = 0
		}
	}()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config storageproto.Config
	if err = decoder.Decode(&config); err != nil || storageproto.ValidateConfig(config) != nil {
		return storageproto.Config{}, errors.New("invalid config file")
	}
	return config, nil
}

func fail(jsonOutput bool, report storageproto.ConformanceReport) {
	if jsonOutput {
		_ = json.NewEncoder(os.Stdout).Encode(report)
	} else {
		for _, check := range report.Checks {
			fmt.Printf("FAIL %s\n", check.Name)
		}
	}
	os.Exit(1)
}
