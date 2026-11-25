// Command discovery-directory runs one HTTPS rendezvous directory instance.
// Each independent provider must use its own host, TLS identity, and data root.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/integrated-recorder/core/internal/discovery"
)

const (
	defaultListenAddress = "127.0.0.1:8443"
	shutdownTimeout      = 10 * time.Second
)

type config struct {
	listenAddress string
	dataRoot      string
	certificate   string
	privateKey    string
}

type startupError struct{ stage string }

func (e startupError) Error() string { return "discovery directory " + e.stage + " failed" }

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Getenv); err != nil {
		// Do not print raw filesystem, TLS, or request errors. The stage is a
		// stable operator diagnostic; detailed private paths stay local.
		log.Printf("%v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, getenv func(string) string) error {
	cfg, err := loadConfig(getenv)
	if err != nil {
		return err
	}
	if _, err := tls.LoadX509KeyPair(cfg.certificate, cfg.privateKey); err != nil {
		return startupError{stage: "TLS certificate validation"}
	}
	directory, err := discovery.OpenDirectory(cfg.dataRoot)
	if err != nil {
		return startupError{stage: "durable state initialization"}
	}
	server := newServer(cfg.listenAddress, directory)
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServeTLS(cfg.certificate, cfg.privateKey)
	}()
	log.Printf("discovery directory listening over HTTPS on %s", cfg.listenAddress)
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return startupError{stage: "graceful shutdown"}
		}
		if err := <-serverErrors; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return startupError{stage: "HTTPS listener"}
		}
		return nil
	case err := <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
			return nil
		}
		return startupError{stage: "HTTPS listener"}
	}
}

func loadConfig(getenv func(string) string) (config, error) {
	if getenv == nil {
		return config{}, startupError{stage: "configuration"}
	}
	cfg := config{
		listenAddress: strings.TrimSpace(getenv("IR_DISCOVERY_LISTEN_ADDR")),
		dataRoot:      strings.TrimSpace(getenv("IR_DISCOVERY_DATA_DIR")),
		certificate:   strings.TrimSpace(getenv("IR_DISCOVERY_TLS_CERT")),
		privateKey:    strings.TrimSpace(getenv("IR_DISCOVERY_TLS_KEY")),
	}
	if cfg.listenAddress == "" {
		cfg.listenAddress = defaultListenAddress
	}
	host, port, err := net.SplitHostPort(cfg.listenAddress)
	portNumber, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || portNumber < 1 || portNumber > 65535 || strings.ContainsAny(host, "\r\n") || strings.ContainsAny(cfg.listenAddress, "\r\n") {
		return config{}, startupError{stage: "configuration"}
	}
	if !filepath.IsAbs(cfg.dataRoot) || filepath.Clean(cfg.dataRoot) != cfg.dataRoot || cfg.dataRoot == string(filepath.Separator) {
		return config{}, startupError{stage: "configuration"}
	}
	if cfg.certificate == "" || cfg.privateKey == "" || cfg.certificate == cfg.privateKey {
		return config{}, startupError{stage: "configuration"}
	}
	return cfg, nil
}

func newServer(address string, directory *discovery.Directory) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("/", discovery.HTTPHandler(directory))
	return &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
}
