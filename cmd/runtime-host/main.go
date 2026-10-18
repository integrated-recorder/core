// Command runtime-host is PID 1 in the production image. It owns the stable
// external listener and supervises separate Control and Recorder Engine
// processes from the immutable image-bundled application release.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/integrated-recorder/core/internal/authn"
	"github.com/integrated-recorder/core/internal/management"
	"github.com/integrated-recorder/core/internal/runtimehost/bootstrap"
)

func main() {
	if len(os.Args) == 3 && os.Args[1] == "reset-password" {
		dataDir := os.Getenv("DATA_DIR")
		if dataDir == "" {
			dataDir = "/data"
		}
		if err := resetPasswordFromLocalConsole(dataDir, os.Args[2], os.Stdin, os.Stdout); err != nil {
			log.Printf("runtime-host: password recovery failed; check the local recovery instructions")
			os.Exit(1)
		}
		return
	}
	if len(os.Args) != 1 {
		log.Printf("runtime-host: supported command: reset-password <login>")
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

func resetPasswordFromLocalConsole(dataDir, login string, in *os.File, out io.Writer) error {
	password, confirmation, err := readRecoveryPasswords(in, out)
	defer func() {
		for i := range password {
			password[i] = 0
		}
		for i := range confirmation {
			confirmation[i] = 0
		}
	}()
	if err != nil || !bytes.Equal(password, confirmation) {
		return authn.ErrInvalidCredentials
	}
	service, err := authn.OpenWithoutBootstrap(dataDir)
	if err != nil {
		return err
	}
	user, err := service.RecoverPassword(login, string(password))
	if err != nil {
		return err
	}
	if err := management.AppendLocalAdminAudit(dataDir, "password_recovered", user.ID); err != nil {
		return errors.New("password was updated but the security audit could not be recorded")
	}
	_, err = fmt.Fprintln(out, "Password reset completed.")
	return err
}
