//go:build unix

package worker

import "syscall"

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
