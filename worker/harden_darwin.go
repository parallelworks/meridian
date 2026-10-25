package worker

import (
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/parallelworks/meridian/problems"
)

// harden keeps the user's other processes, a job's steps among them, from
// reading the worker's memory, where its credential is: it refuses
// debuggers, which would attach to it (PT_DENY_ATTACH), and sets the size
// of a core file of it to 0, as macOS's default is, so that nothing makes
// it dump core. Reading another process's memory any other way takes
// root, or a debugger the user has authorised. A process the worker starts
// starts with that core file size too, and can raise it.
func harden() error {
	if err := unix.PtraceDenyAttach(); err != nil {
		return problems.WorkerHardenFailed.Problem(problems.Params{"reason": err.Error()})
	}
	var core syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_CORE, &core)
	if err == nil {
		core.Cur = 0
		err = syscall.Setrlimit(syscall.RLIMIT_CORE, &core)
	}
	if err != nil {
		return problems.WorkerHardenFailed.Problem(problems.Params{"reason": err.Error()})
	}
	return nil
}
