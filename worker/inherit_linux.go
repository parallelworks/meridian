//go:build linux

package worker

import (
	"math"

	"golang.org/x/sys/unix"
)

// closeOnExec marks every descriptor from 3 close-on-exec: with
// close_range, on Linux 5.11 and later, or one by one.
func closeOnExec() {
	if unix.CloseRange(3, math.MaxUint32, unix.CLOSE_RANGE_CLOEXEC) == nil {
		return
	}
	if !closeOnExecListed("/proc/self/fd") {
		closeOnExecUpTo()
	}
}

// runningExecutable returns the path that opens the file the process
// runs: /proc/self/exe, which is that file even if another has taken its
// path since, as an upgrade in place does.
func runningExecutable() (string, error) { return "/proc/self/exe", nil }
