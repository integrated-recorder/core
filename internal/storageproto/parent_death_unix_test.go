//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly || solaris

package storageproto

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const parentDeathHelperEnv = "IR_STORAGEPROTO_PARENT_DEATH_HELPER"

// TestProviderParentDeathHelper runs as a separate parent process so the test
// can hard-kill it and prove Start's child does not become an orphan.
func TestProviderParentDeathHelper(t *testing.T) {
	if os.Getenv(parentDeathHelperEnv) != "1" {
		return
	}
	dir := os.Getenv("IR_STORAGEPROTO_PARENT_DEATH_DIR")
	binary := os.Getenv("IR_STORAGEPROTO_PARENT_DEATH_BINARY")
	client, err := Start(context.Background(), StartOptions{
		Binary:     binary,
		SocketPath: filepath.Join(dir, "provider.sock"),
		TokenFile:  filepath.Join(dir, "token"),
	})
	if err != nil {
		t.Fatalf("start provider: %v", err)
	}
	pidPath := filepath.Join(dir, "provider.pid")
	tmpPIDPath := pidPath + ".tmp"
	if err = os.WriteFile(tmpPIDPath, []byte(strconv.Itoa(client.cmd.Process.Pid)), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(tmpPIDPath, pidPath); err != nil {
		t.Fatal(err)
	}
	select {}
}

func TestProviderChildExitsWhenParentIsHardKilled(t *testing.T) {
	if testing.Short() {
		t.Skip("builds an external provider and helper process")
	}
	dir, err := os.MkdirTemp("/tmp", "ir-spv1-parent-death-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	token := filepath.Join(dir, "token")
	if err = os.WriteFile(token, []byte("0123456789abcdef0123456789abcdef"), 0600); err != nil {
		t.Fatal(err)
	}
	binary := buildFixture(t)
	helper := exec.Command(os.Args[0], "-test.run=^TestProviderParentDeathHelper$")
	helper.Env = append(os.Environ(),
		parentDeathHelperEnv+"=1",
		"IR_STORAGEPROTO_PARENT_DEATH_DIR="+dir,
		"IR_STORAGEPROTO_PARENT_DEATH_BINARY="+binary,
	)
	helper.Stdout = nil
	helper.Stderr = nil
	if err = helper.Start(); err != nil {
		t.Fatal(err)
	}
	helperDone := make(chan struct{})
	var helperErr error
	go func() {
		helperErr = helper.Wait()
		close(helperDone)
	}()
	defer func() {
		if helper.Process != nil && !processDone(helperDone) {
			_ = helper.Process.Kill()
			<-helperDone
		}
	}()

	pidPath := filepath.Join(dir, "provider.pid")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, statErr := os.Stat(pidPath); statErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("helper did not publish provider PID")
		}
		select {
		case <-helperDone:
			t.Fatalf("helper exited before provider start: %v", helperErr)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	pidBytes, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	providerPID, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if err != nil || providerPID <= 0 {
		t.Fatalf("invalid provider PID: %q", pidBytes)
	}
	if err = helper.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-helperDone

	deadline = time.Now().Add(8 * time.Second)
	for processExists(providerPID) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if processExists(providerPID) {
		t.Fatalf("provider process %d survived hard parent termination", providerPID)
	}
}

func processDone(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func processExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}
