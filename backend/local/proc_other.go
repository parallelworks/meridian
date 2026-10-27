//go:build !linux && !darwin && !freebsd

package local

import (
	"os/exec"
	"runtime"

	"github.com/parallelworks/foundation/problem"

	"github.com/parallelworks/meridian/problems"
)

// unsupported says that the driver doesn't run workers here: the worker
// runs only on Linux, macOS and FreeBSD, where it can keep the user's
// other processes out of its memory.
func unsupported() *problem.Problem {
	return problems.WorkerSystemUnsupported.Problem(problems.Params{"os": runtime.GOOS})
}

// What follows is never called here, where unsupported stops every call
// first.

const openNoFollow = 0

func detach(*exec.Cmd) {}

type signal int

const (
	sigTerm signal = iota
	sigKill
)

func signalWorker(*record, signal) error { return unsupported() }

func socketpair() ([2]int, error) { return [2]int{}, unsupported() }

func runningExecutable(string) (string, error) { return "", unsupported() }

func processStart(int) (string, error) { return "", unsupported() }

func readBootID() (string, error) { return "", unsupported() }

func hostMemory() (int64, error) { return 0, unsupported() }

func hostMilliCPU() int64 { return 0 }
