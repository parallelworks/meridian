package worker

import (
	"errors"
	"runtime"

	process "github.com/parallelworks/meridian/internal/harden"
	"github.com/parallelworks/meridian/problems"
)

// harden keeps the user's other processes, a job's steps among them, from
// reading the worker's memory, where its credential is, or attaching to
// it (internal/harden): worker_harden_failed when the system refuses, and
// worker_system_unsupported on a system that gives a process no way to.
func harden() error {
	switch err := process.Process(); {
	case errors.Is(err, process.ErrUnsupported):
		return problems.WorkerSystemUnsupported.Problem(problems.Params{"os": runtime.GOOS})
	case err != nil:
		return problems.WorkerHardenFailed.Problem(problems.Params{"reason": err.Error()})
	}
	return nil
}
