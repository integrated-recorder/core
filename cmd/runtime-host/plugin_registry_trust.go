//go:build !runtime_e2e

package main

import "github.com/integrated-recorder/core/internal/runtimehost/bootstrap"

// Production hosts always use the normal public HTTPS client. The local
// registry trust seam exists only in runtime_e2e builds.
func configureRuntimeE2EPluginRegistryTrust(*bootstrap.Config) error { return nil }
