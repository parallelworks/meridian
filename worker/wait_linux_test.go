//go:build linux && !mips && !mipsle && !mips64 && !mips64le

package worker

import (
	"os/exec"
	"syscall"
	"testing"
)

// TestWaitExitHolds checks that waiting for a step's process gives its
// exit status as exec.ExitError does, and leaves it unreaped, so that its
// process group's number stays its own, until cmd.Wait reaps it.
func TestWaitExitHolds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		script string
		want   int
	}{
		{"exit 0", 0},
		{"exit 7", 7},
		{"kill -TERM $$", -1},
	} {
		cmd := exec.Command("/bin/sh", "-c", tc.script)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		pid := cmd.Process.Pid
		if got := waitExit(cmd); got != tc.want {
			t.Errorf("%q: exit status %d, want %d", tc.script, got, tc.want)
		}
		if err := syscall.Kill(pid, 0); err != nil {
			t.Errorf("%q: the process was reaped: %v", tc.script, err)
		}
		if alive := liveGroupsOf([]int{pid}); alive[pid] {
			t.Errorf("%q: a group whose only process is its zombie leader is live", tc.script)
		}
		if got := exitOf(cmd.Wait()); got != tc.want {
			t.Errorf("%q: cmd.Wait gives %d, want %d", tc.script, got, tc.want)
		}
		if err := syscall.Kill(pid, 0); err == nil {
			t.Errorf("%q: the process is still there once reaped", tc.script)
		}
	}
}
