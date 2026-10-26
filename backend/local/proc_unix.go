//go:build linux || darwin || freebsd

package local

import (
	"os"
	"os/exec"
	"runtime"
	"syscall"

	"github.com/parallelworks/foundation/problem"
)

// unsupported is nil where the driver runs workers.
func unsupported() *problem.Problem { return nil }

// openNoFollow opens a file only if it isn't a symbolic link.
const openNoFollow = syscall.O_NOFOLLOW

// detach starts cmd in a session of its own, so that the signals of the
// terminal the placing process runs in, Ctrl-C's SIGINT and SIGHUP, don't
// reach it.
func detach(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}

// The signals Cancel sends.
const (
	sigTerm = syscall.SIGTERM
	sigKill = syscall.SIGKILL
)

// lockFile takes an exclusive lock on the file at path, which it makes if
// need be, waiting for whoever holds it, and returns what releases it.
func lockFile(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|openNoFollow, 0o600) //nolint:gosec // an attempt's lock, in placements/
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil { //nolint:gosec // a descriptor fits in an int
		f.Close()
		return nil, err
	}
	return func() { f.Close() }, nil
}

// socketpair returns the descriptors of a new stream Unix socket's ends,
// each closed on exec, so that only a process given one, as a worker its
// own, has it.
func socketpair() ([2]int, error) {
	syscall.ForkLock.RLock()
	defer syscall.ForkLock.RUnlock()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return fds, err
	}
	syscall.CloseOnExec(fds[0])
	syscall.CloseOnExec(fds[1])
	return fds, nil
}

// hostMilliCPU is the host's CPUs, in thousandths of a core.
func hostMilliCPU() int64 { return int64(runtime.NumCPU()) * 1000 }
