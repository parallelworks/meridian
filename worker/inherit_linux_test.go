//go:build linux

package worker

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// childEnv makes the test binary a child that TestInheritNothing starts,
// which prints whether a step it starts ignores SIGHUP, after
// inheritNothing if its value is reset.
const childEnv = "WORKER_TEST_INHERIT_CHILD"

func init() {
	mode := os.Getenv(childEnv)
	if mode == "" {
		return
	}
	if mode == "reset" {
		inheritNothing()
	}
	out, err := exec.Command("/bin/sh", "-c", "grep '^SigIgn:' /proc/self/status").Output() //nolint:noctx // a test's child, which exits next
	if err != nil {
		os.Exit(2)
	}
	os.Stdout.Write(out)
	os.Exit(0)
}

// TestInheritNothing checks that a worker started with SIGHUP ignored, as
// under nohup, starts its steps with SIGHUP's default handling, and that
// it marks every descriptor it inherited close-on-exec.
func TestInheritNothing(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ignoresHUP := func(mode string) bool {
		t.Helper()
		// The shell ignores SIGHUP, and the child starts ignoring it.
		cmd := exec.CommandContext(t.Context(), "/bin/sh", "-c", `trap "" HUP; exec "$0"`, exe)
		cmd.Env = append(os.Environ(), childEnv+"="+mode)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		mask, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(string(out), "SigIgn:")), 16, 64)
		if err != nil {
			t.Fatalf("%s: %q", mode, out)
		}
		return mask&(1<<(uint(syscall.SIGHUP)-1)) != 0
	}
	if !ignoresHUP("keep") {
		t.Fatal("a step of a worker started ignoring SIGHUP doesn't, before inheritNothing: the test shows nothing")
	}
	if ignoresHUP("reset") {
		t.Error("a step of a worker started ignoring SIGHUP ignores it too")
	}

	fd, err := unix.Open("/dev/null", unix.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil || flags&unix.FD_CLOEXEC != 0 {
		t.Fatalf("a descriptor opened without close-on-exec: %d, %v", flags, err)
	}
	closeOnExec()
	if flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Errorf("after closeOnExec, the descriptor's flags are %d, %v", flags, err)
	}
}
