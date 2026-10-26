//go:build linux || darwin

package worker

import (
	"os"
	"strconv"
	"syscall"
)

// closeOnExecListed marks close-on-exec each descriptor from 3 that dir
// lists, as /proc/self/fd and /dev/fd do, and reports false if it can't
// list them.
func closeOnExecListed(dir string) bool {
	f, err := os.Open(dir) //nolint:gosec // /proc/self/fd or /dev/fd
	if err != nil {
		return false
	}
	names, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return false
	}
	for _, name := range names {
		if fd, err := strconv.Atoi(name); err == nil && fd >= 3 {
			syscall.CloseOnExec(fd)
		}
	}
	return true
}
