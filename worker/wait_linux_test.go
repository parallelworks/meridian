//go:build linux && !mips && !mipsle && !mips64 && !mips64le

package worker

import (
	"bytes"
	"os"
	"os/exec"
	"strconv"
	"strings"
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

// TestNoZombiesLeft checks that once Run returns, the worker has reaped
// every step's process group leader it held, though a step left a process
// in its group. It doesn't run in parallel, as other tests' workers hold
// leaders of their own.
func TestNoZombiesLeft(t *testing.T) {
	f := newFakeCoordinator(t, newJob(command(`nohup sleep 1 >/dev/null 2>&1 </dev/null &`), command("sleep 1.5")))
	if err := f.runWorker(t.Context(), Config{}); err != nil {
		t.Fatal(err)
	}
	me := strconv.Itoa(os.Getpid())
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Skip("no /proc")
	}
	for _, e := range entries {
		data, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		_, after, ok := bytes.Cut(data, []byte(") "))
		if !ok {
			continue
		}
		if fields := strings.Fields(string(after)); len(fields) > 1 && fields[0] == "Z" && fields[1] == me {
			t.Errorf("a zombie child, %s, is left", e.Name())
		}
	}
}
