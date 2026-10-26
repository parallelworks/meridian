//go:build freebsd

package worker

import (
	"os"

	"golang.org/x/sys/unix"
)

// closeRangeCloexec is CLOSE_RANGE_CLOEXEC, from <unistd.h>.
const closeRangeCloexec = 1 << 2

// closeOnExec marks every descriptor from 3 close-on-exec: with
// close_range, on FreeBSD 13.1 and later, or one by one.
func closeOnExec() {
	if _, _, errno := unix.Syscall(unix.SYS_CLOSE_RANGE, 3, ^uintptr(0)>>1, closeRangeCloexec); errno == 0 {
		return
	}
	closeOnExecUpTo()
}

// runningExecutable returns the path of the file the process runs.
func runningExecutable() (string, error) { return os.Executable() }
