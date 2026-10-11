package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

func readRecoveryPasswords(in *os.File, out io.Writer) ([]byte, []byte, error) {
	if in == nil || out == nil {
		return nil, nil, fmt.Errorf("password input is unavailable")
	}
	if terminalDescriptor(int(in.Fd())) {
		if _, err := fmt.Fprint(out, "New password: "); err != nil {
			return nil, nil, err
		}
		password, err := readTerminalPassword(int(in.Fd()))
		_, _ = fmt.Fprintln(out)
		if err != nil {
			return nil, nil, err
		}
		if _, err := fmt.Fprint(out, "Confirm new password: "); err != nil {
			return password, nil, err
		}
		confirmation, err := readTerminalPassword(int(in.Fd()))
		_, _ = fmt.Fprintln(out)
		return password, confirmation, err
	}
	if _, err := fmt.Fprintln(out, "Enter the new password twice on stdin (one line each):"); err != nil {
		return nil, nil, err
	}
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 128), 128)
	if !scanner.Scan() {
		return nil, nil, fmt.Errorf("password input is incomplete")
	}
	password := []byte(scanner.Text())
	if !scanner.Scan() {
		return password, nil, fmt.Errorf("password confirmation is incomplete")
	}
	return password, []byte(scanner.Text()), nil
}

func readTerminalPassword(fd int) ([]byte, error) {
	original, err := readTerminalAttributes(fd)
	if err != nil {
		return nil, fmt.Errorf("secure terminal input is unavailable")
	}
	hidden := *original
	hidden.Lflag &^= syscallEcho
	if err := writeTerminalAttributes(fd, &hidden); err != nil {
		return nil, fmt.Errorf("secure terminal input is unavailable")
	}
	defer func() { _ = writeTerminalAttributes(fd, original) }()
	result := make([]byte, 0, 72)
	tooLong := false
	var one [1]byte
	for {
		count, readErr := syscallRead(fd, one[:])
		if readErr != nil {
			return nil, fmt.Errorf("password input is incomplete")
		}
		if count == 0 {
			return nil, fmt.Errorf("password input is incomplete")
		}
		if one[0] == '\n' {
			break
		}
		if one[0] == '\r' {
			continue
		}
		if len(result) < 73 {
			result = append(result, one[0])
		} else {
			tooLong = true
		}
	}
	if tooLong || len(result) > 72 {
		for i := range result {
			result[i] = 0
		}
		return nil, fmt.Errorf("password input is too long")
	}
	return result, nil
}
