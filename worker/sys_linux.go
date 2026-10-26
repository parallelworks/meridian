//go:build linux

package worker

import (
	"syscall"

	"github.com/parallelworks/meridian/problems"
)

// harden keeps the user's other processes, a job's steps among them, from
// reading the worker's memory, where its credential is, or attaching to
// it: it makes the worker not dumpable, which also keeps it from dumping
// core. A process it starts is dumpable again once it runs its program.
func harden() error {
	if _, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, syscall.PR_SET_DUMPABLE, 0, 0); errno != 0 {
		return problems.WorkerHardenFailed.Problem(problems.Params{"reason": errno.Error()})
	}
	return nil
}
