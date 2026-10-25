//go:build darwin || freebsd || openbsd

package worker

import (
	"os"
	"os/exec"
	"testing"
)

// TestHarden checks that a worker can keep the user's other processes
// out of its memory, where its credential is. It runs in a process of its
// own, since it would change the tests'.
func TestHarden(t *testing.T) {
	if os.Getenv("MERIDIAN_TEST_HARDEN") == "1" {
		if err := harden(); err != nil {
			t.Fatal(err)
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
