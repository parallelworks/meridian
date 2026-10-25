package worker

import (
	"os"
	"runtime"
	"syscall"
	"unsafe"

	"github.com/parallelworks/meridian/problems"
)

// What procctl takes, from <sys/wait.h> and <sys/procctl.h>, which package
// syscall doesn't have.
const (
	pPID                = 0 // P_PID
	procTraceCtl        = 7 // PROC_TRACE_CTL
	procTraceCtlDisable = 2 // PROC_TRACE_CTL_DISABLE
)

// harden keeps the user's other processes, a job's steps among them, from
// attaching to the worker, reading its memory, where its credential is,
// or tracing it, and keeps it from dumping core: procctl's
// PROC_TRACE_CTL_DISABLE. A process it starts inherits that until it runs
// its program.
func harden() error {
	arg := int32(procTraceCtlDisable)
	pid := uintptr(os.Getpid())
	var errno syscall.Errno
	// procctl's id is an id_t, of 64 bits: on a 32-bit system it takes two
	// of a system call's words, and on ARM it starts at an even one.
	switch {
	case unsafe.Sizeof(uintptr(0)) == 8:
		_, _, errno = syscall.Syscall6(syscall.SYS_PROCCTL, pPID, pid, procTraceCtl, uintptr(unsafe.Pointer(&arg)), 0, 0) //nolint:gosec // procctl reads the int it is given
	case runtime.GOARCH == "arm":
		_, _, errno = syscall.Syscall6(syscall.SYS_PROCCTL, pPID, 0, pid, 0, procTraceCtl, uintptr(unsafe.Pointer(&arg))) //nolint:gosec // procctl reads the int it is given
	default:
		_, _, errno = syscall.Syscall6(syscall.SYS_PROCCTL, pPID, pid, 0, procTraceCtl, uintptr(unsafe.Pointer(&arg)), 0) //nolint:gosec // procctl reads the int it is given
	}
	if errno != 0 {
		return problems.WorkerHardenFailed.Problem(problems.Params{"reason": errno.Error()})
	}
	return nil
}
