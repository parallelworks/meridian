//go:build unix

package sockownertest

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// ListenAs starts the test's own binary again as the user UID, which
// listens at a port of 127.0.0.1 that the system picks (Main), and returns
// the port, at which it listens until the test ends. Only root can start
// a process as another user, so it skips the test for any other, unless
// RootEnv is set, as where CI runs the test as root, which the test then
// fails for. The binary is copied first to a directory of its own in
// /tmp, which every user can reach, as the one go test builds it in, and
// $TMPDIR, as macOS sets it, needn't be.
func ListenAs(t *testing.T) int {
	t.Helper()
	if os.Getuid() != 0 {
		if os.Getenv(RootEnv) != "" {
			t.Fatalf("%s is set, but the test runs as user %d, not root", RootEnv, os.Getuid())
		}
		t.Skip("only root can listen as another user")
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("/tmp", "sockownertest")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o755); err != nil { //nolint:gosec // UID runs the binary in it
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "listener")
	if err := copyBinary(self, bin); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), bin, "-test.run=^$") //nolint:gosec // the test's own binary
	cmd.Dir = dir
	// UID can't write the coverage of the binary go test asked for.
	cmd.Env = append(slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "GOCOVERDIR=") }), listenEnv+"=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: UID, Gid: UID}}
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Wait()
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("the listener as user %d said nothing: %v", UID, err)
	}
	port, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		t.Fatalf("the listener as user %d said %q", UID, line)
	}
	return port
}

// copyBinary copies the executable from to to, which anyone can run.
func copyBinary(from, to string) error {
	in, err := os.Open(from) //nolint:gosec // the test's own binary
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o755) //nolint:gosec // UID runs it
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
