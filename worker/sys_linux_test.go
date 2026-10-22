//go:build linux

package worker

import (
	"os"
	"os/exec"
	"syscall"
	"testing"
)

// TestHarden checks that a worker makes itself not dumpable, so that the
// user's other processes can't read its memory, where its credential is.
// It runs in a process of its own, since it would change the tests'.
func TestHarden(t *testing.T) {
	if os.Getenv("MERIDIAN_TEST_HARDEN") == "1" {
		if err := harden(); err != nil {
			t.Fatal(err)
		}
		if dumpable, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, syscall.PR_GET_DUMPABLE, 0, 0); errno != 0 || dumpable != 0 {
			t.Fatalf("dumpable is %d (%v)", dumpable, errno)
		}
		return
	}
	t.Parallel()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHarden$", "-test.count=1") //nolint:gosec // the test's own binary
	cmd.Env = append(os.Environ(), "MERIDIAN_TEST_HARDEN=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
}
