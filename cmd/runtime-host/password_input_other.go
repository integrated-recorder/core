//go:build !darwin && !linux

package main

import "syscall"

func terminalDescriptor(int) bool              { return false }
func readTerminalPassword(int) ([]byte, error) { return nil, syscall.ENOTSUP }
