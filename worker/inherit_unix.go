//go:build unix

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

// closeOnExecUpTo marks close-on-exec each descriptor from 3 below the
// process's limit, or the first 65536, for a system that can't list them.
func closeOnExecUpTo() {
	limit := uint64(1 << 16)
	var rl syscall.Rlimit
	if syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl) == nil && uint64(rl.Cur) < limit { //nolint:gosec,unconvert // its type differs by system
		limit = uint64(rl.Cur) //nolint:gosec,unconvert // as above
	}
	for fd := 3; uint64(fd) < limit; fd++ {
		syscall.CloseOnExec(fd)
	}
}
