//go:build linux || darwin || freebsd

package local

import (
	"errors"
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

// signalProcess sends sig to the process pid, and errNoProcess if it isn't
// there.
func signalProcess(pid int, sig syscall.Signal) error {
	err := syscall.Kill(pid, sig)
	if errors.Is(err, syscall.ESRCH) {
		return errNoProcess
	}
	return err
}

// hostMilliCPU is the host's CPUs, in thousandths of a core.
func hostMilliCPU() int64 { return int64(runtime.NumCPU()) * 1000 }
