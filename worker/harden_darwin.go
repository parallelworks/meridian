package worker

import (
	"golang.org/x/sys/unix"

	"github.com/parallelworks/meridian/problems"
)

// harden keeps the user's other processes, a job's steps among them, from
// attaching to the worker as a debugger does, to read its memory, where
// its credential is: PT_DENY_ATTACH. Reading another process's memory
// any other way takes root, or a debugger the user has authorised, and
// macOS writes no core file unless the user's limits ask for one.
func harden() error {
	if err := unix.PtraceDenyAttach(); err != nil {
		return problems.WorkerHardenFailed.Problem(problems.Params{"reason": err.Error()})
	}
	return nil
}
