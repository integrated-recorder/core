//go:build linux

package main

import (
	"syscall"
	"unsafe"
)

const syscallEcho = syscall.ECHO

func readTerminalAttributes(fd int) (*syscall.Termios, error) {
	var state syscall.Termios
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uintptr(syscall.TCGETS), uintptr(unsafe.Pointer(&state)))
	if errno != 0 {
		return nil, errno
	}
	return &state, nil
}

func writeTerminalAttributes(fd int, state *syscall.Termios) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), uintptr(syscall.TCSETS), uintptr(unsafe.Pointer(state)))
	if errno != 0 {
		return errno
	}
	return nil
}

func terminalDescriptor(fd int) bool {
	_, err := readTerminalAttributes(fd)
	return err == nil
}

func syscallRead(fd int, data []byte) (int, error) { return syscall.Read(fd, data) }
